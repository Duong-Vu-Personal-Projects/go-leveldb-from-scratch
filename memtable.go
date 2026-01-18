package go_leveldb_from_scratch_own

import (
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
		data: skiplist.New(skiplist.Bytes),
	}
}
func (m *MemTable) Put(key, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	//if exists -> update
	if oldElement := m.data.Get(key); oldElement != nil {
		oldValue := oldElement.Value.([]byte)
		m.size -= len(key) + len(oldValue)
	}
	m.data.Set(key, value)
	m.size += len(key) + len(value)
}
func (m *MemTable) Get(key []byte) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	element := m.data.Get(key)
	if element != nil {
		return element.Value.([]byte), true
	}
	return nil, false
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
