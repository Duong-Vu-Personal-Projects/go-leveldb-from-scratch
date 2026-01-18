package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"math"
	"os"
	"sort"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/huandu/skiplist"
)

const (
	//DataBlockSize groups key-value pairs into block of this size
	DataBlockSize   = 1 * 1024 * 4 //4KB
	FooterBlockSize = 4
)

// IndexEntry stores the last key of a data block and its location in SSTable file
type IndexEntry struct {
	LastKey InternalKey
	Offset  int64
	Size    int
}

// Footer stores the location of the index and filter block
type Footer struct {
	IndexOffset  int64
	IndexSize    int
	FilterOffset int64
	FilterSize   int
}
type SSTableReader struct {
	file   *os.File
	index  []IndexEntry
	filter *bloom.BloomFilter
	cmp    internalKeyComparable
}

func WriteSSTable(path string, itemCount uint, it *skiplist.Element) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	var indexEntries []IndexEntry
	var currentOffset int64 = 0
	filter := bloom.NewWithEstimates(itemCount, 0.01)
	blockBuffer := new(bytes.Buffer)
	var lastKeyInBlock InternalKey

	for ; it != nil; it = it.Next() {
		internalKey := it.Key().(InternalKey)
		value := it.Value.([]byte)
		filter.Add([]byte(internalKey.UserKey))
		if blockBuffer.Len() > DataBlockSize {
			//write data block to SSTable file
			blockBytes := blockBuffer.Bytes()
			n, err := writer.Write(blockBytes)
			if err != nil {
				return err
			}
			indexEntries = append(indexEntries, IndexEntry{
				LastKey: lastKeyInBlock,
				Offset:  currentOffset,
				Size:    n,
			})
			currentOffset += int64(n)
			blockBuffer.Reset()
		}
		keyBuf := new(bytes.Buffer)
		if err := gob.NewEncoder(keyBuf).Encode(internalKey); err != nil {
			return err
		}
		keyBytes := keyBuf.Bytes()
		binary.Write(blockBuffer, binary.LittleEndian, uint32(len(keyBytes)))
		binary.Write(blockBuffer, binary.LittleEndian, uint32(len(value)))
		blockBuffer.Write(keyBytes)
		blockBuffer.Write(value)
		lastKeyInBlock = internalKey
	}
	if blockBuffer.Len() > 0 {
		blockBytes := blockBuffer.Bytes()
		n, err := writer.Write(blockBytes)
		if err != nil {
			return err
		}
		indexEntries = append(indexEntries, IndexEntry{
			LastKey: lastKeyInBlock,
			Offset:  currentOffset,
			Size:    n,
		})
		currentOffset += int64(n)
	}
	//write the filter block
	filterOffset := currentOffset
	filterSize, err := filter.WriteTo(writer)
	if err != nil {
		return err
	}
	//write the index block
	indexOffset := currentOffset + filterSize
	if err := writer.Flush(); err != nil {
		return err
	}
	indexBuf := new(bytes.Buffer)
	if err := gob.NewEncoder(indexBuf).Encode(indexEntries); err != nil {
		return err
	}
	indexBytes := indexBuf.Bytes()
	if _, err := writer.Write(indexBytes); err != nil {
		return err
	}
	indexSize := len(indexBytes)
	//write the footer
	footer := Footer{
		IndexOffset:  indexOffset,
		IndexSize:    indexSize,
		FilterOffset: filterOffset,
		FilterSize:   int(filterSize),
	}
	footerBuffer := new(bytes.Buffer)
	if err := gob.NewEncoder(footerBuffer).Encode(footer); err != nil {
		return err
	}
	footerBytes := footerBuffer.Bytes()
	if _, err := writer.Write(footerBytes); err != nil {
		return err
	}
	footerSizeUint := uint32(len(footerBytes))
	if err := binary.Write(writer, binary.LittleEndian, footerSizeUint); err != nil {
		return err
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	return file.Sync()
}

func (r *SSTableReader) Get(userKey []byte) ([]byte, bool, error) {
	if !r.filter.Test(userKey) {
		return nil, false, nil
	}
	searchKey := InternalKey{
		UserKey: string(userKey),
		SeqNum:  math.MaxInt64,
		Type:    OpTypePut,
	}
	// find the data block that contains this searchKey
	blockIndex := sort.Search(len(r.index), func(i int) bool {
		return r.cmp.Compare(r.index[i].LastKey, searchKey) >= 0
	})
	if blockIndex >= len(r.index) {
		return nil, false, nil
	}
	entry := r.index[blockIndex]
	blockData := make([]byte, entry.Size)
	_, err := r.file.ReadAt(blockData, entry.Offset)
	if err != nil {
		return nil, false, err
	}
	reader := bytes.NewReader(blockData)
	for {
		var keySize, valueSize uint32
		if err := binary.Read(reader, binary.LittleEndian, &keySize); err != nil {
			if err == io.EOF {
				break
			}
			return nil, false, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &valueSize); err != nil {
			return nil, false, err
		}
		keyBytes := make([]byte, keySize)
		if _, err := io.ReadFull(reader, keyBytes); err != nil {
			return nil, false, err
		}
		var ik InternalKey
		if err := gob.NewDecoder(bytes.NewReader(keyBytes)).Decode(&ik); err != nil {
			//corrupted key, skip this entry
			reader.Seek(int64(valueSize), io.SeekCurrent)
			continue
		}
		if ik.UserKey == string(userKey) {
			//found the latest version of user key
			if ik.Type == OpTypeDelete {
				return nil, true, nil
			}
			valueBuf := make([]byte, valueSize)
			if _, err := io.ReadFull(reader, valueBuf); err != nil {
				return nil, false, err
			}
			return valueBuf, true, nil
		}
		//key didn't match, so skip over the value to get to the next entry
		if _, err := reader.Seek(int64(valueSize), io.SeekCurrent); err != nil {
			return nil, false, err
		}
	}
	return nil, false, nil
}

// Construct an in-memory reader by reading metadata from the SSTable file tail
// so you can do fast lookups (use filter + index to find a data block).
func NewSSTableReader(path string) (*SSTableReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	//read the footerSize
	fileSize := stat.Size()
	footerSizeBuf := make([]byte, FooterBlockSize)
	if _, err := file.ReadAt(footerSizeBuf, fileSize-FooterBlockSize); err != nil {
		return nil, fmt.Errorf("failed to read footer size: %w", err)
	}
	footerSize := binary.LittleEndian.Uint32(footerSizeBuf)
	//read the footer
	footerOffset := fileSize - FooterBlockSize - int64(footerSize)
	footerBuf := make([]byte, footerSize)
	if _, err := file.ReadAt(footerBuf, footerOffset); err != nil {
		return nil, fmt.Errorf("failed to read footer: %w", err)
	}
	var footer Footer
	if err := gob.NewDecoder(bytes.NewReader(footerBuf)).Decode(&footer); err != nil {
		return nil, fmt.Errorf("failed to decode footer: %w", err)
	}
	//read the filter block
	filterBuf := make([]byte, footer.FilterSize)
	if _, err := file.ReadAt(filterBuf, footer.FilterOffset); err != nil {
		return nil, fmt.Errorf("failed to read filter block: %w", err)
	}
	var filter = &bloom.BloomFilter{}
	if _, err := filter.ReadFrom(bytes.NewReader(filterBuf)); err != nil {
		return nil, fmt.Errorf("failed to read from filter buffer: %w", err)
	}
	//read the index block
	indexBuf := make([]byte, footer.IndexSize)
	if _, err := file.ReadAt(indexBuf, footer.IndexOffset); err != nil {
		return nil, fmt.Errorf("failed to read index block: %w", err)
	}
	var index []IndexEntry
	if err := gob.NewDecoder(bytes.NewReader(indexBuf)).Decode(&index); err != nil {
		return nil, fmt.Errorf("failed to decode index: %w", err)
	}
	return &SSTableReader{
		file:   file,
		index:  index,
		filter: filter,
		cmp:    internalKeyComparable{},
	}, nil
}
