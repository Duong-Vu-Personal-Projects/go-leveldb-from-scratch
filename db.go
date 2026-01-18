package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
)

const (
	MemTableSizeThreshold = 1 * 1024 * 4 //4KB
	stateFileName         = "state.json"
	activeWalFileName     = "db.wal"
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
	mu           sync.RWMutex
	wal          *WAL
	mem          *MemTable
	immutableMem *MemTable //hold the memtable data being flushed

	dataDir        string
	ssTableCounter int
	//global sequence number for all operations
	sequenceNum atomic.Uint64
}

// NewDB creates or opens a database at the specified path.
// It first replays all WALs to recover the state
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
	mem := NewMemTable()
	var maxSeqNum uint64 = 0
	// List all WAL files and sort them in order so that we replay in the order they were created.
	// Imagine this situation:
	// - Flush #1 triggered: memtable is full, flushMemtable is called
	// - WAL rotation: in side flushMemtable:
	//   - db.wal is renamed to wal-00001.log
	//   - a new db.wal is created
	//   - the full memtable is moved to immutableMem
	//   - lock is released
	walFiles, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	sort.Strings(walFiles)
	activeWal := filepath.Join(dir, activeWalFileName)
	walFiles = append(walFiles, activeWal)
	for _, walPath := range walFiles {
		if _, err := os.Stat(walPath); os.IsNotExist(err) {
			continue
		}
		recoveredData, lastSeq, err := Replay(walPath)
		if err != nil {
			return nil, err
		}
		if lastSeq > maxSeqNum {
			maxSeqNum = lastSeq
		}
		for key, value := range recoveredData {
			mem.Put(key, value.Value)
		}
	}
	log.Printf("Recovery complete. Highest sequence number is %d", maxSeqNum)
	wal, err := NewWal(activeWal)
	if err != nil {
		return nil, err
	}
	db := &DB{
		wal:            wal,
		mem:            mem,
		dataDir:        dir,
		ssTableCounter: state.SSTableCounter,
	}
	db.sequenceNum.Store(maxSeqNum)
	err = db.saveState()
	if err != nil {
		return nil, err
	}
	return db, nil
}
func (db *DB) flushMemtable() {
	//prevent other operations while flushing

	log.Println("Memtable is full, starting flush...")
	db.mu.Lock()
	if db.immutableMem != nil {
		db.mu.Unlock()
		return
	}
	//WAL rotation
	walPath := db.wal.file.Name()
	rotatedWalPath := fmt.Sprintf("%s/wal-%05d.log", db.dataDir, db.ssTableCounter)
	db.wal.Close()
	if err := os.Rename(walPath, rotatedWalPath); err != nil {
		log.Printf("CRITICAL: Failed to rename WAL: %v", err)
		db.mu.Unlock()
		return
	}
	newWal, err := NewWal(walPath)
	if err != nil {
		log.Printf("CRITICAL ERROR: Failed to open new WAL: %v", err)
		db.mu.Unlock()
		return
	}
	db.wal = newWal
	db.immutableMem = db.mem
	db.mem = NewMemTable()
	db.mu.Unlock()

	go func(imm *MemTable, walToDelete string) {
		log.Println("Background flush: Starting to write SSTable...")
		sstablePath := fmt.Sprintf("%s/%05d.sst", db.dataDir, db.ssTableCounter)
		db.ssTableCounter++
		itemCount := imm.data.Len()
		if err := WriteSSTable(sstablePath, uint(itemCount), imm.data.Front()); err != nil {
			log.Printf("ERROR: Failed to write SSTable: %v", err)
			return
		}
		log.Printf("Successfully flushed memtable to %s", sstablePath)
		db.mu.Lock()
		defer db.mu.Unlock()
		db.immutableMem = nil
		if err := db.saveState(); err != nil {
			log.Printf("CRITICAL ERROR: Failed to save state file: %v", err)
			return
		}

		log.Println("Truncating WAL file...")
		if err := os.Remove(walToDelete); err != nil {
			log.Printf("ERROR: Failed to delete rotated WAL %s: %v", walToDelete, err)
		} else {
			log.Printf("Background flush: Deleted old WAL %s", walToDelete)
		}

		return
	}(db.immutableMem, rotatedWalPath)
}
func (db *DB) Put(key, value []byte) error {
	seqNum := db.sequenceNum.Add(1)
	internalKey := InternalKey{
		UserKey: string(key),
		SeqNum:  seqNum,
		Type:    OpTypePut,
	}
	entry := LogEntry{
		Op:     OpPut,
		Key:    key,
		Value:  value,
		SeqNum: seqNum,
	}
	db.mu.RLock()
	wal := db.wal
	memTable := db.mem
	db.mu.RUnlock()
	if err := wal.Write(&entry); err != nil {
		return err
	}

	memTable.Put(internalKey, value)

	if memTable.ApproximateSize() > MemTableSizeThreshold {
		db.flushMemtable()
	}
	return nil

}
func (db *DB) Get(key []byte) ([]byte, bool) {
	db.mu.RLock()
	mem := db.mem
	imm := db.immutableMem
	counter := db.ssTableCounter
	db.mu.RUnlock()
	//1.check in active memtable
	val, found := mem.Get(key)
	if found {
		if val == nil {
			//delete log, not have value
			return nil, false
		}
		return val, true
	}
	//2.check in immutable memtable
	if imm != nil {
		val, found = imm.Get(key)
		if found {
			if val == nil {
				// Found a delete tombstone
				return nil, false
			}
			return val, true
		}
	}
	log.Printf("sstable count: %d", counter)
	//3.search key in newest to oldest SSTables
	for i := db.ssTableCounter - 1; i > 0; i-- {
		ssTablePath := fmt.Sprintf("%s/%05d.sst", db.dataDir, i)
		reader, err := NewSSTableReader(ssTablePath)
		if err != nil {
			log.Printf("Error opening SSTable reader for %s: %v", ssTablePath, err)
			continue
		}
		val, found, err := reader.Get(key)
		if err != nil {
			log.Printf("Error reading SSTable %s: %v", ssTablePath, err)
			continue
		}
		if found {
			if val == nil {
				return nil, false
			}
			return val, true
		}
	}
	return nil, false
}
func (db *DB) Delete(key []byte) error {
	seqNum := db.sequenceNum.Add(1)
	internalKey := InternalKey{
		UserKey: string(key),
		SeqNum:  seqNum,
		Type:    OpTypeDelete,
	}
	entry := &LogEntry{
		Op:     OpDelete,
		Key:    key,
		SeqNum: seqNum,
	}
	db.mu.RLock()
	wal := db.wal
	memTable := db.mem
	db.mu.RUnlock()
	if err := wal.Write(entry); err != nil {
		return err
	}
	memTable.Put(internalKey, nil)
	if memTable.ApproximateSize() > MemTableSizeThreshold {
		db.flushMemtable()
	}
	return nil
}
func (db *DB) Close() error {
	return db.wal.Close()
}
