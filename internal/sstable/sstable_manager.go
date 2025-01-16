package sstable

import (
	"encoding/binary"
	"fmt"
	"log"
)

const (
	SSTablesPath = "../data/sstables/"
)

func HandleError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %v", msg, err)
	}
}

type SSTableManager struct {
	Capacity uint32
	List     []*SSTable
}

func NewSSTableManager() *SSTableManager {
	return &SSTableManager{List: make([]*SSTable, 0)}
}


func (manager *SSTableManager) AddSSTable(sstable *SSTable) {
	// LRU algoritam za izbacivanje, do kapaciteta punimo
	if uint32(len(manager.List)) == manager.Capacity {
		manager.List = manager.List[1:]
	}
	manager.List = append(manager.List, sstable)
}

func (manager *SSTableManager) Get(filename string) *SSTable {    // vraća sstable po imenu npr "sstable_00003"
	for _, sstable := range manager.List {
		if sstable.SSTableName == filename {
			return sstable
		}
	}
	return nil
}


// uint64 u varint
func Uint64toVarint(value uint64) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, value)
	return buf[:n]
}

// varint u uint64
func VarintToUint64(buf []byte) (uint64, error) {
	value, n := binary.Uvarint(buf)
	if n <= 0 {
		return 0, fmt.Errorf("UvarintToUint64 failed")
	}
	return value, nil
}

// uint32 u varint
func Uint32toVarint(value uint32) []byte {
	return Uint64toVarint(uint64(value))
}

// varint u uint32
func VarintToUint32(buf []byte) (uint32, error) {
	value, err := VarintToUint64(buf)
	if err != nil {
		return 0, err
	}
	if value > uint64(^uint32(0)) {
		return 0, fmt.Errorf("VarintToUint32 failed - value too large")
	}
	return uint32(value), nil
}
