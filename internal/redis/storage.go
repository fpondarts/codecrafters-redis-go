package redis

import (
	"time"
)

type Record struct {
	Value      string
	Expiration time.Time
}
type Storage struct {
	storage map[string]Record
}

func NewStorage() *Storage {
	return &Storage{storage: make(map[string]Record)}
}

func (s *Storage) Get(key string) (Record, bool) {
	r, ok := s.storage[key]
	if !ok {
		return Record{}, false
	}
	if !r.Expiration.IsZero() && time.Now().After(r.Expiration) {
		delete(s.storage, key)
		return Record{}, false
	}
	return r, true
}

func (s *Storage) Set(key, val string, expiration time.Time) {
	s.storage[key] = Record{Value: val, Expiration: expiration}
}
