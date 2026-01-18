package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
)

const (
	MemTableSizeThreshold = 1 * 1024 * 4 //4KB
	stateFileName         = "state.json"
)

type DBState struct {
	SSTableCounter int `json:"sstable_counter"`
}

// saveState serializes the current DB state to a json file
func (db *DB) saveState() error {
	state := DBState{
		SSTableCounter: db.ssTableCounter,
	}
	data, err := json.MarshalIndent(state, "", "\t")
	if err != nil {
		return err
	}
	statePath := filepath.Join(db.dataDir, stateFileName)
	return os.WriteFile(statePath, data, 0644)
}

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
	statePath := filepath.Join(dir, stateFileName)
	var state DBState
	data, err := os.ReadFile(statePath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Println("State file not found, initializing with default state...")
			state = DBState{
				SSTableCounter: 1,
			}
		} else {
			return nil, err
		}
	} else {
		if err := json.Unmarshal(data, &state); err != nil {
			return nil, err
		}
		log.Printf("Loaded state: SSTableCounter is %d", state.SSTableCounter)
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
		ssTableCounter: state.SSTableCounter,
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
	if err := db.saveState(); err != nil {
		log.Printf("CRITICAL: Failed to save state file: %v", err)
		return err
	}
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
	db.mu.RLock()
	val, ok := db.mem.Get(key)
	db.mu.RUnlock()
	if ok {
		return val, true
	}
	log.Printf("sstable count: %d", db.ssTableCounter)
	//search key in newest to oldest SSTables
	for i := db.ssTableCounter - 1; i > 0; i-- {
		ssTablePath := fmt.Sprintf("%s/%05d.sst", db.dataDir, i)
		val, found, err := FindInSSTable(ssTablePath, key)
		if err != nil {
			log.Printf("Error reading SSTable %s: %v", ssTablePath, err)
			continue
		}
		if found {
			return val, true
		}
	}
	return nil, false
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
