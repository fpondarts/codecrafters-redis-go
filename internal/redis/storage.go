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
	ID     string
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

	finalID, err := resolveStreamID(id, stream)
	if err != nil {
		return "", err
	}

	stream = append(stream, StreamEntry{ID: finalID, Fields: fields})
	s.storage[key] = record{vtype: streamType, val: stream, expiration: r.expiration}
	return finalID, nil
}

func resolveStreamID(id string, entries []StreamEntry) (string, error) {
	var lastMS, lastSeq uint64
	if len(entries) > 0 {
		lastMS, lastSeq, _ = parseStreamID(entries[len(entries)-1].ID)
	}

	nowMS := uint64(time.Now().UnixMilli())

	switch {
	case id == "*":
		ms := nowMS
		seq := uint64(0)
		if ms == lastMS {
			seq = lastSeq + 1
		}
		return fmt.Sprintf("%d-%d", ms, seq), nil

	case strings.HasSuffix(id, "-*"):
		ms, err := strconv.ParseUint(strings.TrimSuffix(id, "-*"), 10, 64)
		if err != nil {
			return "", fmt.Errorf("ERR Invalid stream ID")
		}
		seq := uint64(0)
		if ms == lastMS {
			seq = lastSeq + 1
		} else if ms < lastMS {
			return "", fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}
		return fmt.Sprintf("%d-%d", ms, seq), nil

	default:
		ms, seq, err := parseStreamID(id)
		if err != nil {
			return "", fmt.Errorf("ERR Invalid stream ID specified as stream command argument")
		}
		if ms == 0 && seq == 0 {
			return "", fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}
		if len(entries) > 0 && (ms < lastMS || (ms == lastMS && seq <= lastSeq)) {
			return "", fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}
		return id, nil
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
