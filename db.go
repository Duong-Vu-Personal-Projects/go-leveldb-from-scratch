package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sync"
)

const (
	MemTableSizeThreshold = 1 * 1024 * 4 //4KB
)

type DB struct {
	mu  sync.RWMutex
	wal *WAL
	mem *MemTable

	dataDir        string
	ssTableCounter int
}

func NewDB(dir string) (*DB, error) {
	//first, replay the WAL to recover the state
	//file mode 0755: https://www.warp.dev/terminus/chmod-755
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	walPath := fmt.Sprintf("%s/db.wal", dir)
	recoveredData, err := Replay(walPath)
	if err != nil {
		return nil, err
	}
	log.Printf("Recovered %d entries from WAL", len(recoveredData))

	//create a new memtable and fill with recovered data
	mem := NewMemTable()
	for key, value := range recoveredData {
		mem.Put([]byte(key), value)
	}
	wal, err := NewWal(walPath)
	if err != nil {
		return nil, err
	}
	return &DB{
		wal:            wal,
		mem:            mem,
		dataDir:        dir,
		ssTableCounter: 1,
	}, nil
}
func (db *DB) flushMemtable() error {
	//prevent other operations while flushing
	db.mu.Lock()
	defer db.mu.Unlock()

	log.Println("Memtable is full, starting flush...")

	//set memtable to immutable memtable
	immutableMemTable := db.mem
	db.mem = NewMemTable()

	//write the immutable memtable to a new SSTable in the background
	//do it synchronously here for simplicity
	ssTablePath := fmt.Sprintf("%s/%05d.sst", db.dataDir, db.ssTableCounter)
	db.ssTableCounter++

	if err := WriteSSTable(ssTablePath, immutableMemTable.data.Front()); err != nil {
		log.Printf("ERROR: Failed to write SSTable: %v", err)
		db.mem = immutableMemTable
		return err
	}
	log.Printf("Successfully flushed memtable to %s", ssTablePath)
	log.Println("Truncating WAL file...")
	if err := db.wal.Close(); err != nil {
		log.Printf("CRITICAL: Failed to close old WAL file: %v", err)
		return err
	}
	//re-open the wal file with the truncate flag to clear it
	walPath := fmt.Sprintf("%s/db.wal", db.dataDir)
	flag := os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	fileMode := os.FileMode(0644)
	file, err := os.OpenFile(walPath, flag, fileMode)
	if err != nil {
		log.Printf("CRITICAL: Failed to create new WAL file: %v", err)
		return err
	}
	db.wal = &WAL{
		file: file,
		bw:   bufio.NewWriter(file),
	}
	return nil
}
func (db *DB) Put(key, value []byte) error {
	entry := LogEntry{
		Op:    OpPut,
		Key:   key,
		Value: value,
	}
	if err := db.wal.Write(&entry); err != nil {
		return err
	}
	db.mu.RLock()
	db.mem.Put(key, value)
	currentSize := db.mem.ApproximateSize()
	db.mu.RUnlock()
	if currentSize > MemTableSizeThreshold {
		if err := db.flushMemtable(); err != nil {
			return err
		}
	}
	return nil

}
func (db *DB) Get(key []byte) ([]byte, bool) {
	return db.mem.Get(key)
}
func (db *DB) Delete(key []byte) error {
	entry := LogEntry{
		Op:  OpDelete,
		Key: key,
	}
	if err := db.wal.Write(&entry); err != nil {
		return err
	}
	db.mem.Delete(key)
	return nil
}
func (db *DB) Close() error {
	return db.wal.Close()
}
