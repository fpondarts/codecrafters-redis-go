package redis

import (
	"errors"
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

func (s *Storage) Set(key, val string, expiration time.Time) error {
	if r, ok := s.storage[key]; ok && !r.isExpired() && r.vtype != stringType {
		return ErrWrongType
	}
	s.storage[key] = record{vtype: stringType, strVal: val, expiration: expiration}
	return nil
}

func (s *Storage) Get(key string) (string, bool, error) {
	r, ok := s.storage[key]
	if !ok || r.isExpired() {
		delete(s.storage, key)
		return "", false, nil
	}
	if r.vtype != stringType {
		return "", false, ErrWrongType
	}
	return r.strVal, true, nil
}

func (s *Storage) RPush(key string, vals ...string) (int, error) {
	if r, ok := s.storage[key]; ok && !r.isExpired() && r.vtype != listType {
		return 0, ErrWrongType
	}
	r := s.storage[key]
	r.vtype = listType
	r.listVal = append(r.listVal, vals...)
	s.storage[key] = r
	return len(r.listVal), nil
}

func (s *Storage) GetListRange(key string, start, inclusiveEnd int) ([]string, error) {
	if r, ok := s.storage[key]; ok && !r.isExpired() && r.vtype != listType {
		return []string{}, ErrWrongType
	}

	if start > inclusiveEnd {
		return []string{}, nil
	}

	r := s.storage[key]

	if start >= len(r.listVal) {
		return []string{}, nil
	}

	if start < 0 {
		if start < -len(r.listVal) {
			start = 0
		} else {
			start = len(r.listVal) + start
		}
	}

	if inclusiveEnd < 0 {
		if inclusiveEnd < -len(r.listVal) {
			inclusiveEnd = 0
		} else {
			inclusiveEnd = len(r.listVal) + inclusiveEnd
		}
	}

	inclusiveEnd = min(inclusiveEnd, len(r.listVal)-1)

	return r.listVal[start : inclusiveEnd+1], nil
}
