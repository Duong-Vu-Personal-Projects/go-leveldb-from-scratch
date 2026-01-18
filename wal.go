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
	Op    byte
	Key   []byte
	Value []byte
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
// Header =[Key Size (4 bytes)] [Value Size (4 bytes)] [Operation (1 byte)]
// KV = [Key][Value]
func (w *WAL) Write(entry *LogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	keySize := len(entry.Key)
	valueSize := len(entry.Value)

	//Total size: key_size(4) + value_size(4) + op(1) + key + value
	entrySize := 4 + 4 + 1 + keySize + valueSize
	buf := make([]byte, entrySize)

	//encode the entry fields into the buffer
	binary.LittleEndian.PutUint32(buf[0:4], uint32(keySize))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(valueSize))
	buf[8] = entry.Op
	copy(buf[9:9+keySize], entry.Key)
	copy(buf[9+keySize:], entry.Value)
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

// Replay read all entries from the WAL file at the given path and reconstruct
// the in-memory state by replaying the operations
func Replay(path string) (map[string][]byte, error) {
	//open the file for reading only
	flag := os.O_RDONLY
	mode := os.FileMode(0644)
	file, err := os.OpenFile(path, flag, mode)
	if err != nil {
		//if the file doesn't exist, meaning no data to recover, which is not an error
		if os.IsNotExist(err) {
			return make(map[string][]byte), nil
		}
		return nil, err

	}
	defer file.Close()
	data := make(map[string][]byte)
	reader := bufio.NewReader(file)

	for {
		//1.read and verify checksum
		var storedChecksum uint32
		err := binary.Read(reader, binary.LittleEndian, &storedChecksum)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		//2.read sizes
		var keySize, valueSize uint32
		if err := binary.Read(reader, binary.LittleEndian, &keySize); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &valueSize); err != nil {
			return nil, err
		}
		//3.read the rest of the data (op + key+value)
		entryDataSize := 1 + int(keySize) + int(valueSize)
		entryData := make([]byte, entryDataSize)
		if _, err := io.ReadFull(reader, entryData); err != nil {
			return nil, err
		}
		//4.combine headers and data to verify checksum
		headerData := make([]byte, 8) //4 for keySize, 4 for valueSize
		binary.LittleEndian.PutUint32(headerData[0:4], keySize)
		binary.LittleEndian.PutUint32(headerData[4:8], valueSize)
		fullData := append(headerData, entryData...)

		actualChecksum := crc32.ChecksumIEEE(fullData)
		if storedChecksum != actualChecksum {
			return nil, fmt.Errorf("data corruption: checksum mismatch")
		}
		//5. decode and apply the operation to map
		op := entryData[0]
		key := entryData[1 : 1+keySize]
		value := entryData[1+keySize:]
		if op == OpPut {
			data[string(key)] = value
		} else if op == OpDelete {
			delete(data, string(key))
		}
	}
	return data, nil
}
