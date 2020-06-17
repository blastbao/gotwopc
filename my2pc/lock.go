package main

import (
	"sync"
)

type LockMgr struct {
	locks sync.Map
}

func NewLockMgr() *LockMgr {
	return &LockMgr{
		locks: sync.Map{},
	}
}

func (m *LockMgr) Lock(key string) bool {
	_, locked := m.locks.LoadOrStore(key, struct{}{})
	return locked
}

func (m *LockMgr) Unlock(key string) {
	m.locks.Delete(key)
	return
}