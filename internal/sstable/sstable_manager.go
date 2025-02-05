package sstable

import (
	"NASP-NoSQL-Engine/internal/block_manager"
	"NASP-NoSQL-Engine/internal/config"
	"NASP-NoSQL-Engine/internal/entry"
	"NASP-NoSQL-Engine/internal/probabilistics"
	"NASP-NoSQL-Engine/internal/trees"
	"fmt"
	"log"
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
		
		// dodajemo newline karakter
		// nakon zadnjeg index tuple-a ne dodajemo newline karakter
		
		if string(it.Key) != string(indexTuples[len(indexTuples)-1].Key) {
			index = append(index, 10)
		}

	}

	return index
}

// funkcija koja kreira Summary na osnovu indexa, vraća niz bajtova
func (sstm *SSTableManager) CreateNONMergeSummary(indexTuples []IndexTuple, index []byte) []byte {
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

	// ideja je da na osnovu index strukture napravimo summary npr.
	// INDEX
	/*
		ključ1 -> offset1
		ključ2 -> offset2
		ključ3 -> offset3
		ključ4 -> offset4
	*/
	// SUMMARY
	/*
		ključ1 -> offset1 (ali ne offset1 prekopiran iz indexa, nego offset na kom bajtu počinje ključ1 u indexu, ne u data)
		ključ4 (npr za thinning 3) -> offset2 (ali ne offset2 prekopiran iz indexa, nego offset na kom bajtu počinje ključ4 u indexu, ne u data)
	*/
	// dakle, summary treba da sadrži ključeve i offsete na kojima se ti ključevi nalaze u indexu

	// prolazimo kroz index i upisujemo ključeve i offsete
	// index je niz bajtova, prodjemo kroz index 

	//bytesPassed - updatuje se stalno, to će nam biti offseti u summary-ju
	// prolazimo kroz bajtove, sabiramo ih na bytes passed i kada prodjemo onoliko newline karaktera koliko je thinning, upisujemo ključ i offset (bytes passed)
	// za početak odmah upisujemo prvi ključ i offset 0
	summary = append(summary, []byte(keys[0])...)
	summary = append(summary, 0)
	summary = append(summary, entry.Uint32ToBytes(0)...)
	summary = append(summary, 10)

	currentKeyIndex := 0
	newlinesPassed := 0

	for bytesPassed := 0; bytesPassed < len(index); bytesPassed++ {
		
		if index[bytesPassed] == 10 { // ako je null bajt
			newlinesPassed++
			currentKeyIndex++
		}

		if newlinesPassed == int(thinning) {
			summary = append(summary, []byte(keys[currentKeyIndex])...)
			summary = append(summary, 0)
			summary = append(summary, entry.Uint32ToBytes(uint32(bytesPassed + 1))...) // brojimo bajtove od 0 zato je plus 1
			summary = append(summary, 10) // za razliku od indexa, ovde svaki red ima newline karakter na kraju (čak i poslednji)
			newlinesPassed = 0
		}
	}	

	return summary
}
