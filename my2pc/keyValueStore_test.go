package main

import (
	"fmt"
	"testing"
	"os"

	"github.com/stretchr/testify/assert"
)

var testDbPath = "/Users/jianweili/go/src/github.com/blastbao/gotwopc/test.db"

func TestTearDown(c *testing.T) {
	os.RemoveAll(testDbPath)
}

func TestGetWithoutPut(c *testing.T) {
	store := newKeyValueStore(testDbPath)
	_, err := store.get("nonexistentvalue")
	if err == nil {
		c.Error("Entry foo should not exist.")
	}
}

func TestDeleteNonexistent(c *testing.T) {
	store := newKeyValueStore(testDbPath)
	err := store.del("nonexistentvalue")
	if err != nil {
		c.Error("KVS should not error when deleting nonexistent value: ", err)
	}
}

func TestKeyValueStoreAll(c *testing.T) {
	store := newKeyValueStore(testDbPath)
	err := store.put("foo", "bar")
	if err != nil {
		c.Fatal("Failed to put:", err)
	}

	val, err := store.get("foo")
	if err != nil {
		c.Fatal("Failed to get:", err)
	}

	assert.Equal(c, val, "bar")

	err = store.del("foo")
	if err != nil {
		c.Fatal("Failed to del:", err)
	}

	val, err = store.get("foo")
	if err == nil {
		c.Error("Entry foo should not exist.")
	}
}

func TestKeyValueStoreMultiple(c *testing.T) {
	store := newKeyValueStore(testDbPath)
	const count = 10

	for i := 0; i < count; i += 1 {
		key := fmt.Sprintf("key%v", i)
		val := fmt.Sprintf("val%v", i)
		err := store.put(key, val)
		if err != nil {
			c.Fatal("Failed to put:", err)
		}
	}

	for i := 0; i < count; i += 1 {
		key := fmt.Sprintf("key%v", i)
		val := fmt.Sprintf("val%v", i)
		v, err := store.get(key)
		if err != nil {
			c.Fatal("Failed to get:", err)
		}

		fmt.Printf("key=%s, val=%s, real v=%s\n", key, val, v)
		assert.Equal(c, v, val)

	}
}

func TestList(c *testing.T) {
	store := newKeyValueStore(testDbPath)
	const count = 10

	for i := 0; i < count; i += 1 {
		store.put(fmt.Sprintf("key%v", i), fmt.Sprintf("val%v", i))
	}

	keys, err := store.list()
	if err != nil {
		c.Fatal("Failed to list:", err)
	}

	for i, key := range keys {
		if key != fmt.Sprintf("key%v", i) {
			c.Fatal("Key #", i, "was", key, "expected", fmt.Sprintf("key%v", i))
		}
	}
}

func BenchmarkKeyValueStorePut(c *testing.B) {
	store := newKeyValueStore(testDbPath)
	for i := 0; i < c.N; i++ {
		key := fmt.Sprintf("key%v", i)
		val := fmt.Sprintf("val%v", i)
		err := store.put(key, val)
		if err != nil {
			c.Fatal("Failed to put:", err)
		}
	}
}
