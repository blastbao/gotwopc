package main

import (
	"sync"
)

type MemStore struct {
	m sync.Map
}

func (s *MemStore) Put(key string, value interface{}) {
	s.m.Store(key, value)
}

func (s *MemStore) Get(key string) (interface{}, bool) {
	return s.m.Load(key)
}

func (s *MemStore) Exist(key string) bool {
	_, ok := s.m.Load(key)
	return ok
}

func (s *MemStore) Del(key string) {
	s.m.Delete(key)
}

func (s *MemStore) Keys() []string {
	var keys []string
	s.m.Range(func(k, v interface{}) bool {
		keys = append(keys, k.(string))
		return true
	})
	return keys
}

func NewMemStore() *MemStore {
	return &MemStore{
		m: sync.Map{},
	}
}
