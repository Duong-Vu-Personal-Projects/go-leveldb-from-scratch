package main

import "log"

type DB struct {
	wal *WAL
	mem *MemTable
}

func NewDB(path string) (*DB, error) {
	//first, replay the WAL to recover the state
	recoveredData, err := Replay(path)
	if err != nil {
		return nil, err
	}
	log.Printf("Recovered %d entries from WAL", len(recoveredData))

	//create a new memtable and fill with recovered data
	mem := NewMemTable()
	for key, value := range recoveredData {
		mem.Put([]byte(key), value)
	}
	wal, err := NewWal(path)
	if err != nil {
		return nil, err
	}
	return &DB{
		wal: wal,
		mem: mem,
	}, nil
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
	db.mem.Put(key, value)
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
