package main

import (
	"io/ioutil"
	"log"
	"os"
	"path"
	"sync"
)

type MemStore struct {
	m sync.Map
}

func (s *MemStore) Put(key string, value interface{}) bool {
	_, load := s.m.LoadOrStore(key, value)

	return !load
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

func NewMemStore() *MemStore {
	return &MemStore{
		m: sync.Map{},
	}
}


type keyValueStore struct {
	basePath string
}

func newKeyValueStore(dbPath string) (store *keyValueStore) {
	err := os.MkdirAll(dbPath, 0777)
	if err != nil {
		log.Fatalln("newKeyValueStore:", err)
	}
	store = &keyValueStore{dbPath}
	return
}

func (s *keyValueStore) getPath(key string) string {
	return path.Join(s.basePath, key)
}

func (s *keyValueStore) put(key string, value string) (err error) {
	err = ioutil.WriteFile(s.getPath(key), []byte(value), 0777)
	return
}

func (s *keyValueStore) del(key string) (err error) {
	os.Remove(s.getPath(key))
	return nil
}

func (s *keyValueStore) get(key string) (value string, err error) {
	bytes, err := ioutil.ReadFile(s.getPath(key))
	if err != nil {
		return
	}
	value = string(bytes)
	return
}

// 获取所有 keys
func (s *keyValueStore) list() (keys []string, err error) {

	files, err := ioutil.ReadDir(s.basePath)
	if err != nil {
		return nil, err
	}
	keys = make([]string, len(files))
	for i, file := range files {
		keys[i] = file.Name()
	}
	return keys, nil
}

