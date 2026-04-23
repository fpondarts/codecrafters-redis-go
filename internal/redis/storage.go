package redis

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
)

var ErrWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")

type valueType int

const (
	stringType valueType = iota
	listType
	streamType
)

type StreamEntry struct {
	MS     uint64
	Seq    uint64
	Fields []string // alternating key/value pairs
}

type record struct {
	vtype      valueType
	val        any // string | []string | []StreamEntry
	expiration time.Time
}

func (r record) isExpired() bool {
	return !r.expiration.IsZero() && time.Now().After(r.expiration)
}

type Storage struct {
	storage map[string]record
}

func NewStorage() *Storage {
	return &Storage{storage: make(map[string]record)}
}

// getRecord looks up key and validates its type. Returns (zero, false, nil) if the
// key doesn't exist or has expired, (zero, false, ErrWrongType) if the type mismatches,
// and (record, true, nil) on success.
func (s *Storage) getRecord(key string, vtype valueType) (record, bool, error) {
	r, ok := s.storage[key]
	if !ok || r.isExpired() {
		delete(s.storage, key)
		return record{}, false, nil
	}
	if r.vtype != vtype {
		return record{}, false, ErrWrongType
	}
	return r, true, nil
}

func (s *Storage) Set(key, val string, expiration time.Time) error {
	if _, _, err := s.getRecord(key, stringType); err != nil {
		return err
	}
	s.storage[key] = record{vtype: stringType, val: val, expiration: expiration}
	return nil
}

func (s *Storage) Get(key string) (string, bool, error) {
	r, ok, err := s.getRecord(key, stringType)
	if err != nil || !ok {
		return "", false, err
	}
	return r.val.(string), true, nil
}

func (s *Storage) RPush(key string, vals ...string) (int, error) {
	r, _, err := s.getRecord(key, listType)
	if err != nil {
		return 0, err
	}
	list, _ := r.val.([]string)
	list = append(list, vals...)
	s.storage[key] = record{vtype: listType, val: list, expiration: r.expiration}
	return len(list), nil
}

func (s *Storage) LPush(key string, vals ...string) (int, error) {
	r, _, err := s.getRecord(key, listType)
	if err != nil {
		return 0, err
	}
	reversed := slices.Clone(vals)
	slices.Reverse(reversed)
	list, _ := r.val.([]string)
	list = slices.Insert(list, 0, reversed...)
	s.storage[key] = record{vtype: listType, val: list, expiration: r.expiration}
	return len(list), nil
}

func (s *Storage) LLen(key string) (int, error) {
	r, ok, err := s.getRecord(key, listType)
	if err != nil || !ok {
		return 0, err
	}
	return len(r.val.([]string)), nil
}

func (s *Storage) LRange(key string, start, inclusiveEnd int) ([]string, error) {
	r, ok, err := s.getRecord(key, listType)
	if err != nil || !ok {
		return []string{}, err
	}
	list := r.val.([]string)
	if start >= len(list) {
		return []string{}, nil
	}
	if start < 0 {
		start = max(len(list)+start, 0)
	}
	if inclusiveEnd < 0 {
		inclusiveEnd = max(len(list)+inclusiveEnd, 0)
	}
	if start > inclusiveEnd {
		return []string{}, nil
	}
	inclusiveEnd = min(inclusiveEnd, len(list)-1)
	return list[start : inclusiveEnd+1], nil
}

func (s *Storage) LPop(key string, amount int) ([]string, error) {
	r, ok, err := s.getRecord(key, listType)
	if err != nil || !ok {
		return []string{}, err
	}
	list := r.val.([]string)
	safeAmount := min(amount, len(list))
	popped := list[:safeAmount]
	s.storage[key] = record{vtype: listType, val: list[safeAmount:], expiration: r.expiration}
	return popped, nil
}

func (s *Storage) XAdd(key, id string, fields []string) (string, error) {
	r, _, err := s.getRecord(key, streamType)
	if err != nil {
		return "", err
	}
	stream, _ := r.val.([]StreamEntry)

	ms, seq, err := resolveStreamID(id, stream)
	if err != nil {
		return "", err
	}

	stream = append(stream, StreamEntry{MS: ms, Seq: seq, Fields: fields})
	s.storage[key] = record{vtype: streamType, val: stream, expiration: r.expiration}
	return fmt.Sprintf("%d-%d", ms, seq), nil
}

func (s *Storage) XRange(key, startID, endID string) ([]StreamEntry, error) {
	r, ok, err := s.getRecord(key, streamType)
	if err != nil {
		return []StreamEntry{}, err
	}

	if !ok {
		return []StreamEntry{}, err
	}
	stream, _ := r.val.([]StreamEntry)
	startMs, startSeq, err := parseStreamRangeID(startID, true)
	if err != nil {
		return []StreamEntry{}, err
	}

	endMs, endSeq, err := parseStreamRangeID(endID, false)
	if err != nil {
		return []StreamEntry{}, err
	}

	result := []StreamEntry{}

	if startMs > endMs || (startMs == endMs && startSeq > endSeq) {
		return result, nil
	}

	for _, entry := range stream {
		afterStart := entry.MS > startMs || (entry.MS == startMs && entry.Seq >= startSeq)
		beforeEnd := entry.MS < endMs || (entry.MS == endMs && entry.Seq <= endSeq)

		if afterStart && beforeEnd {
			result = append(result, entry)
		}

		if !beforeEnd {
			break
		}
	}

	return result, nil
}

func (s *Storage) XRead(key, startID string) ([]StreamEntry, error) {
	r, ok, err := s.getRecord(key, streamType)
	if err != nil {
		return []StreamEntry{}, err
	}

	if !ok {
		return []StreamEntry{}, nil
	}

	stream := r.val.([]StreamEntry)

	startMs, startSeq, err := parseStreamRangeID(startID, true)
	if err != nil {
		return []StreamEntry{}, err
	}

	index := slices.IndexFunc(stream, func(e StreamEntry) bool {
		return e.MS > startMs || (e.MS == startMs && e.Seq > startSeq)
	})

	if index == -1 {
		return []StreamEntry{}, nil
	}

	return stream[index:], nil
}

func resolveStreamID(id string, entries []StreamEntry) (ms, seq uint64, err error) {
	var lastMS, lastSeq uint64
	if len(entries) > 0 {
		last := entries[len(entries)-1]
		lastMS, lastSeq = last.MS, last.Seq
	}

	nowMS := uint64(time.Now().UnixMilli())

	switch {
	case id == "*":
		ms = nowMS
		if ms == lastMS {
			seq = lastSeq + 1
		}
		return ms, seq, nil

	case strings.HasSuffix(id, "-*"):
		ms, err = strconv.ParseUint(strings.TrimSuffix(id, "-*"), 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("ERR Invalid stream ID")
		}
		if ms < lastMS {
			return 0, 0, fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}
		if ms == lastMS {
			seq = lastSeq + 1
		}
		return ms, seq, nil

	default:
		ms, seq, err = parseStreamID(id)
		if err != nil {
			return 0, 0, fmt.Errorf("ERR Invalid stream ID specified as stream command argument")
		}
		if ms == 0 && seq == 0 {
			return 0, 0, fmt.Errorf("ERR The ID specified in XADD must be greater than 0-0")
		}
		if len(entries) > 0 && (ms < lastMS || (ms == lastMS && seq <= lastSeq)) {
			return 0, 0, fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}
		return ms, seq, nil
	}
}

func parseStreamID(id string) (ms, seq uint64, err error) {
	parts := strings.SplitN(id, "-", 2)
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid ID format")
	}
	ms, err = strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	seq, err = strconv.ParseUint(parts[1], 10, 64)
	return ms, seq, err
}

func parseStreamRangeID(id string, isStart bool) (ms uint64, seq uint64, err error) {
	if id == "-" {
		return 0, 0, nil
	}

	if id == "+" {
		maxuint := ^uint64(0)
		return maxuint, maxuint, nil
	}
	if strings.Contains(id, "-") {
		return parseStreamID(id)
	}

	ms, err = strconv.ParseUint(id, 10, 64)
	if err != nil {
		return 0, 0, err
	}
	if isStart {
		return ms, 0, nil
	}

	maxu64 := ^uint64(0)

	return ms, maxu64, nil
}

func encodeStreamID(ms, seq uint64) string {
	return fmt.Sprintf("%d-%d", ms, seq)
}

func (s *Storage) Type(key string) string {
	r, ok := s.storage[key]
	if !ok || r.isExpired() {
		return ""
	}
	switch r.vtype {
	case listType:
		return "list"
	case streamType:
		return "stream"
	default:
		return "string"
	}
}
