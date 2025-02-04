package sstable

import (
	"log"
	"NASP-NoSQL-Engine/internal/probabilistics"
	"NASP-NoSQL-Engine/internal/trees"
	"NASP-NoSQL-Engine/internal/block_manager"
)

const (
	SSTablesPath = "../data/sstables/"
)

type SSTableManager struct {
	BlockManager *block_manager.BlockManager
	Capacity uint32
	List     []*SSTable
}

type IndexTuple struct {
	Key             []byte
	PositionInBlock uint32
	BlockIndex      uint32
}

type SSTable struct {
	SSTableName string // ime foldera u kome se nalaze svi fajlovi sstable-a
	// merkle pointer      // poredimo korene ako valajju dalje ako ne valjaju moramo tačno locirati položaj
	// bloom filter pointer
	DataName          string
	IndexName         string
	SummaryName       string
	MetadataName      string
	BloomFilterName   string
	BlockSizeFileName string
	MergeName         string
	TOCName           string

	BloomFilter *probabilistics.BloomFilter
	Metadata    *trees.MerkleTree

	BlockSize uint32 // bitno samo prilikom kreiranja sstable-a, kasnije ove podatke čitamo iz fajla
	Merge     bool
}

func HandleError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %v", msg, err)
	}
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
