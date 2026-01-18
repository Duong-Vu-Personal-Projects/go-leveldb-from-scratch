package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

const (
	OpPut byte = iota
	OpDelete
)

// Log Entry represents single operation in the WAL
type LogEntry struct {
	Op     byte
	Key    []byte
	Value  []byte
	SeqNum uint64
}

type WAL struct {
	file *os.File
	mu   sync.Mutex
	bw   *bufio.Writer
}

// NewWAL opens or create a WAL file at the given path
func NewWal(path string) (*WAL, error) {
	//open the file with flags for appending, creating if it not exists and writing
	flag := os.O_APPEND | os.O_WRONLY | os.O_CREATE
	mode := 0644 // user/owner can read, write, cannot execute
	file, err := os.OpenFile(path, flag, os.FileMode(mode))
	if err != nil {
		return nil, err
	}
	return &WAL{
		file: file,
		bw:   bufio.NewWriter(file),
	}, nil
}

// Close WAL file
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.bw.Flush(); err != nil {
		return err
	}
	return w.file.Close()
}

// LogEntry format: crc(4 bytes) - length (2 bytes) - type(1 byte) - data(power of 1 byte)
// Header: 7 bytes - crc(4 bytes) - length (2 bytes) - type(1 byte)
// [crc (4 bytes)] [length (2 bytes, little-endian)] [type (1 byte)] [payload (length bytes)]
// i use slightly different format
// [Checksum(4 bytes)][Header][KV]
// Header =[Seq(8 bytes)][Key Size (4 bytes)] [Value Size (4 bytes)] [Operation (1 byte)]
// KV = [Key][Value]
func (w *WAL) Write(entry *LogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	keySize := len(entry.Key)
	valueSize := len(entry.Value)

	//Total size: seq(8 byte) + key_size(4) + value_size(4) + op(1) + key + value
	entrySize := 8 + 4 + 4 + 1 + keySize + valueSize
	buf := make([]byte, entrySize)

	//encode the entry fields into the buffer
	binary.LittleEndian.PutUint64(buf[0:8], entry.SeqNum)
	binary.LittleEndian.PutUint32(buf[8:12], uint32(keySize))
	binary.LittleEndian.PutUint32(buf[12:16], uint32(valueSize))
	buf[16] = entry.Op
	copy(buf[17:17+keySize], entry.Key)
	copy(buf[17+keySize:], entry.Value)
	//Calculate checksum over the encoded data
	checkSum := crc32.ChecksumIEEE(buf)

	//1.write checksum to the buffer writer
	if err := binary.Write(w.bw, binary.LittleEndian, checkSum); err != nil {
		return err
	}
	//2.write the rest of entry data
	if _, err := w.bw.Write(buf); err != nil {
		return err
	}
	//3.flush the buffer to the file
	//aka moving data from the application buffer to os buffer
	if err := w.bw.Flush(); err != nil {
		return err
	}
	//4. Fsync to guarantee the write to persistent storage
	return w.file.Sync()
}

type RecoveredValue struct {
	Value []byte
	Type  OpType
}

// Replay read all entries from the WAL file at the given path and reconstruct
// the in-memory state by replaying the operations
func Replay(path string) (map[InternalKey]RecoveredValue, uint64, error) {
	//open the file for reading only
	flag := os.O_RDONLY
	mode := os.FileMode(0644)
	file, err := os.OpenFile(path, flag, mode)
	if err != nil {
		//if the file doesn't exist, meaning no data to recover
		if os.IsNotExist(err) {
			return make(map[InternalKey]RecoveredValue), 0, nil
		}
		return nil, 0, err

	}
	defer file.Close()
	data := make(map[InternalKey]RecoveredValue)
	var maxSeqNum uint64 = 0
	reader := bufio.NewReader(file)

	for {
		//1.read and verify checksum
		var storedChecksum uint32
		err := binary.Read(reader, binary.LittleEndian, &storedChecksum)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, 0, err
		}

		//2.read sizes
		headerBuf := make([]byte, 8+4+4+1)
		if _, err := io.ReadFull(reader, headerBuf); err != nil {
			return nil, 0, fmt.Errorf("could not read header: %w", err)
		}
		seqNum := binary.LittleEndian.Uint64(headerBuf[0:8])
		keySize := binary.LittleEndian.Uint32(headerBuf[8:12])
		valueSize := binary.LittleEndian.Uint32(headerBuf[12:16])
		op := headerBuf[16]
		kvBuf := make([]byte, keySize+valueSize)
		if _, err := io.ReadFull(reader, kvBuf); err != nil {
			return nil, 0, fmt.Errorf("could not read key/value: %v", err)
		}

		fullDataPayload := append(headerBuf, kvBuf...)
		actualChecksum := crc32.ChecksumIEEE(fullDataPayload)
		if storedChecksum != actualChecksum {
			return nil, 0, fmt.Errorf("data corruption: checksum mismatch")
		}
		if seqNum > maxSeqNum {
			maxSeqNum = seqNum
		}
		key := kvBuf[:keySize]
		value := kvBuf[keySize:]
		internalKey := InternalKey{
			UserKey: string(key),
			SeqNum:  seqNum,
			Type:    op,
		}
		data[internalKey] = RecoveredValue{
			Value: value,
			Type:  op,
		}
	}
	return data, maxSeqNum, nil
}
