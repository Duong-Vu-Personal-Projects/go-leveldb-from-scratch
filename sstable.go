package main

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"

	"github.com/huandu/skiplist"
)

func WriteSSTable(path string, it *skiplist.Element) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	for ; it != nil; it = it.Next() {
		keyBytes := it.Key().([]byte)
		valueBytes := it.Value.([]byte)
		keySize := uint32(len(keyBytes))
		valueSize := uint32(len(valueBytes))
		if err := binary.Write(file, binary.LittleEndian, keySize); err != nil {
			return err
		}
		if err := binary.Write(file, binary.LittleEndian, valueSize); err != nil {
			return err
		}
		if _, err := file.Write(keyBytes); err != nil {
			return err
		}
		if _, err := file.Write(valueBytes); err != nil {
			return err
		}
	}
	return file.Sync()
}

// FindInSSTable scans an SSTable file to find the value for a given key
func FindInSSTable(path string, key []byte) ([]byte, bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, false, err
	}
	defer file.Close()
	for {
		var keySize, valueSize uint32
		//read key size
		err = binary.Read(file, binary.LittleEndian, &keySize)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, false, err
		}
		//read value size

		if err = binary.Read(file, binary.LittleEndian, &valueSize); err != nil {
			return nil, false, err
		}
		//read key
		keyBuf := make([]byte, keySize)
		if _, err := io.ReadFull(file, keyBuf); err != nil {
			return nil, false, err
		}
		//check if this is the key we are looking for
		if bytes.Equal(keyBuf, key) {
			valueBuf := make([]byte, valueSize)
			if _, err := io.ReadFull(file, valueBuf); err != nil {
				return nil, false, err
			}
			return valueBuf, true, nil
		} else {
			//not our key, skip the value bytes to move to next entry
			if _, err := file.Seek(int64(valueSize), io.SeekCurrent); err != nil {
				return nil, false, err
			}
		}
	}
	return nil, false, nil
}
