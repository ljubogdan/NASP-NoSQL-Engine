package sstable

import (
	"log"
	"NASP-NoSQL-Engine/internal/probabilistics"
	"NASP-NoSQL-Engine/internal/trees"
	"NASP-NoSQL-Engine/internal/block_manager"
	"NASP-NoSQL-Engine/internal/entry"
	"NASP-NoSQL-Engine/internal/config"
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

// funkcija koja kreira Index na osnovu indexTuples, vraća niz bajtova
func (sstm *SSTableManager) CreateNONMergeIndex(indexTuples []IndexTuple, blockSize uint32) []byte {
	index := make([]byte, 0)

	// pravi offset računamo tako što pomnožimo index bloka sa veličinom bloka
	// i dodamo poziciju u bloku
	for _, it := range indexTuples {
		offset := it.BlockIndex * blockSize
		offset += it.PositionInBlock

		index = append(index, []byte(it.Key)...)
		index = append(index, 0)
		index = append(index, entry.Uint32ToBytes(offset)...)
		index = append(index, 10)
	}

	return index
}

// funkcija koja kreira Summary na osnovu indexTuples, vraća niz bajtova
func (sstm *SSTableManager) CreateNONMergeSummary(indexTuples []IndexTuple) []byte {
	summary := make([]byte, 0)

	// čitamo kolika je proredjenost (e.g. 5 znači svaki 5. IndexTuple uzimamo)
	thinning := config.ReadSummaryThinning()

	// za početak prodjemo kroz listu ključeva i vidimo koji ključ je najmanji i koji je najveći, to treba upisati na početku summary-a
	keys := make([]string, 0)
	for _, it := range indexTuples {
		keys = append(keys, string(it.Key))
	}

	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[i] > keys[j] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}

	// upisujemo najmanji i najveći ključ
	summary = append(summary, []byte(keys[0])...)
	summary = append(summary, 0)
	summary = append(summary, []byte(keys[len(keys)-1])...)
	summary = append(summary, 10)

	// prolazimo kroz listu indexTuples i upisujemo ključeve i pozicije u bloku
	for i := 0; i < len(indexTuples); i += int(thinning) {
		summary = append(summary, []byte(indexTuples[i].Key)...)
		summary = append(summary, 0)
		summary = append(summary, entry.Uint32ToBytes(indexTuples[i].PositionInBlock)...)
		summary = append(summary, 10)
	}

	return summary
}
