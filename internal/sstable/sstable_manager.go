package sstable

import (
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
