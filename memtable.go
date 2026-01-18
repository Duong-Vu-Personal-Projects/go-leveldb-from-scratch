package main

import (
	"math"
	"sync"

	"github.com/huandu/skiplist"
)

//MemTable
/*
https://selfboot.cn/en/2025/06/11/leveldb_source_memtable/
In LevelDB, all write operations are first recorded in a Write-Ahead Log
	(WAL) to ensure durability.
The data is then stored in a MemTable. The primary role of the MemTable is to
store recently written data
	in an ordered fashion in memory.
Once certain conditions are met, the data is flushed to disk in batches
*/
type MemTable struct {
	mu   sync.RWMutex
	data *skiplist.SkipList
	size int //approximate size in bytes
}

func NewMemTable() *MemTable {
	return &MemTable{
		data: skiplist.New(internalKeyComparable{}),
	}
}
func (m *MemTable) Put(key InternalKey, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data.Set(key, value)
	m.size += len(key.UserKey) + len(value)
}
func (m *MemTable) Get(key []byte) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	searchKey := InternalKey{
		UserKey: string(key),
		SeqNum:  math.MaxUint64,
		Type:    OpTypePut,
	}
	element := m.data.Find(searchKey)
	if element == nil {
		return nil, false //not found
	}
	foundKey := element.Key().(InternalKey)
	if foundKey.UserKey != string(key) {
		return nil, false //not a match
	}
	if foundKey.Type == OpTypeDelete {
		return nil, true //delete operation, so don't have value
	}
	return element.Value.([]byte), true
}

// remove a key
func (m *MemTable) Delete(key []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if oldElement := m.data.Remove(key); oldElement != nil {
		oldValue := oldElement.Value.([]byte)
		m.size -= len(key) + len(oldValue)
	}
}
func (m *MemTable) ApproximateSize() int {
	return m.size
}
