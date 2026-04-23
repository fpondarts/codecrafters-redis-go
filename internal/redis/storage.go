package redis

import (
	"errors"
	"slices"
	"time"
)

var ErrWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")

type valueType int

const (
	stringType valueType = iota
	listType
)

type record struct {
	vtype      valueType
	strVal     string
	listVal    []string
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
	s.storage[key] = record{vtype: stringType, strVal: val, expiration: expiration}
	return nil
}

func (s *Storage) Get(key string) (string, bool, error) {
	r, ok, err := s.getRecord(key, stringType)
	if err != nil || !ok {
		return "", false, err
	}
	return r.strVal, true, nil
}

func (s *Storage) RPush(key string, vals ...string) (int, error) {
	r, _, err := s.getRecord(key, listType)
	if err != nil {
		return 0, err
	}
	r.vtype = listType
	r.listVal = append(r.listVal, vals...)
	s.storage[key] = r
	return len(r.listVal), nil
}

func (s *Storage) LPush(key string, vals ...string) (int, error) {
	r, _, err := s.getRecord(key, listType)
	if err != nil {
		return 0, err
	}
	reversed := slices.Clone(vals)
	slices.Reverse(reversed)
	r.vtype = listType
	r.listVal = slices.Insert(r.listVal, 0, reversed...)
	s.storage[key] = r
	return len(r.listVal), nil
}

func (s *Storage) LLen(key string) (int, error) {
	r, ok, err := s.getRecord(key, listType)
	if err != nil || !ok {
		return 0, err
	}
	return len(r.listVal), nil
}

func (s *Storage) LRange(key string, start, inclusiveEnd int) ([]string, error) {
	r, ok, err := s.getRecord(key, listType)
	if err != nil || !ok {
		return []string{}, err
	}
	if start >= len(r.listVal) {
		return []string{}, nil
	}
	if start < 0 {
		start = max(len(r.listVal)+start, 0)
	}
	if inclusiveEnd < 0 {
		inclusiveEnd = max(len(r.listVal)+inclusiveEnd, 0)
	}
	if start > inclusiveEnd {
		return []string{}, nil
	}
	inclusiveEnd = min(inclusiveEnd, len(r.listVal)-1)
	return r.listVal[start : inclusiveEnd+1], nil
}

func (s *Storage) LPop(key string, amount int) ([]string, error) {
	r, ok, err := s.getRecord(key, listType)
	if err != nil || !ok {
		return []string{}, err
	}
	safeAmount := min(amount, len(r.listVal))
	popped := r.listVal[:safeAmount]
	r.listVal = r.listVal[safeAmount:]
	s.storage[key] = r
	return popped, nil
}

func (s *Storage) Type(key string) string {
	r, ok := s.storage[key]

	if !ok || r.isExpired() {
		return ""
	}

	if r.vtype == listType {
		return "list"
	}

	return "string"
}
