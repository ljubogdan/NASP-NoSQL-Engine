package sstable

import (
	"NASP-NoSQL-Engine/internal/block_manager"
	"NASP-NoSQL-Engine/internal/config"
	"NASP-NoSQL-Engine/internal/encoded_entry"
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
	Capacity     uint32
	List         []*SSTable
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

func (manager *SSTableManager) Get(filename string) *SSTable { // vraća sstable po imenu npr "sstable_00003"
	for _, sstable := range manager.List {
		if sstable.SSTableName == filename {
			return sstable
		}
	}
	return nil
}

// funkcija koja kreira Index na osnovu indexTuples, vraća niz bajtova
// NAPOMENA - key je već u VARINT formatu
func (sstm *SSTableManager) CreateNONMergeIndex(indexTuples []IndexTuple, blockSize uint32) *[]byte {
	index := make([]byte, 0) 

	// pravi offset računamo tako što pomnožimo index bloka sa veličinom bloka
	// i dodamo poziciju u bloku
	for _, it := range indexTuples {
		offset := it.BlockIndex * blockSize
		offset += it.PositionInBlock

		// appendujemo varint vrednost ključa iz bidirekcione mape
		// appendujemo offset kao varint od već postojećeg 

		index = append(index, it.Key...)
		index = append(index, 0)
		index = append(index, encoded_entry.Uint32toVarint(offset)...)
		index = append(index, 10)

	}

	return &index
}

// funkcija koja kreira Summary na osnovu indexa, vraća niz bajtova
func (sstm *SSTableManager) CreateNONMergeSummary(indexTuples []IndexTuple, index *[]byte) []byte {
	summary := make([]byte, 0)

	// čitamo kolika je proredjenost (e.g. 5 znači svaki 5. IndexTuple uzimamo)
	thinning := config.ReadSummaryThinning()

	// za početak prodjemo kroz listu ključeva i vidimo koji ključ je najmanji i koji je najveći, to treba upisati na početku summary-a
	keys := make([]string, 0)
	for _, it := range indexTuples {
		keyUint32, err := encoded_entry.VarintToUint32(it.Key)
		HandleError(err, "Error converting varint to uint32")
		keys = append(keys, sstm.BlockManager.BidirectionalMap.GetByUint32(keyUint32))
	}

	// printujemo ključeve
	for _, key := range keys {
		log.Println(key)
	}

	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[i] > keys[j] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}

	// upisujemo najmanji i najveći ključ
	summary = append(summary, encoded_entry.Uint32toVarint(sstm.BlockManager.BidirectionalMap.GetByString(keys[0]))...)
	summary = append(summary, 0)
	summary = append(summary, encoded_entry.Uint32toVarint(sstm.BlockManager.BidirectionalMap.GetByString(keys[len(keys)-1]))...)
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

	newlinesPassed := thinning - 1

	/*
	for bytesPassed := 0; bytesPassed < len(*index); bytesPassed++ {

		
		if (*index)[bytesPassed] == 10 { // ako je newline karakter
			newlinesPassed++
			currentKeyIndex++
		}

		if newlinesPassed == int(thinning) {
			summary = append(summary, encoded_entry.Uint32toVarint(sstm.BlockManager.BidirectionalMap.GetByString(keys[currentKeyIndex]))...)
			summary = append(summary, 0)
			summary = append(summary, encoded_entry.Uint32toVarint(uint32(bytesPassed+1))...)
			summary = append(summary, 10)                                            // za razliku od indexa, ovde svaki red ima newline karakter na kraju (čak i poslednji)
			newlinesPassed = 0
		}
		

		// pošto koristimo varint unutar indexa, prvo pročitamo onoliko bajtova koliko je varint (terminiramo sa 0 na 
	}
	*/

	// sada prilagodjavamo algoritam za varint 
	// kada čitamo terminatorni bajt ima 0 na najvišem bitu
	// izgled indexa kroz koji se prolazi je na osnovu povratne vrednosti funkcije CreateNONMergeIndex

	bytesPassed := 0
	for bytesPassed < len(*index) {
		// koristimo isti koncept kao u zakomentarisanoj for petlji iznad
		// samo što sada koristimo ReadVarint funkciju koja vraća pročitane bajtove

		key := encoded_entry.ReadVarint((*index)[bytesPassed:])
		bytesPassed += len(key)
		fmt.Println("Key: ", key)

		// preskačemo terminatorni bajt
		terminator := encoded_entry.ReadVarint((*index)[bytesPassed:])
		bytesPassed += len(terminator)
		fmt.Println("Terminator: ", terminator)

		// čitamo offset
		offset := encoded_entry.ReadVarint((*index)[bytesPassed:])
		bytesPassed += len(offset)
		fmt.Println("Offset: ", offset)

		// preskačemo newline karakter
		newline := encoded_entry.ReadVarint((*index)[bytesPassed:])
		bytesPassed += len(newline)
		fmt.Println("Newline: ", newline)

		newlinesPassed++

		if newlinesPassed == thinning  { // jer smo već pročitali ključ i offset...
			summary = append(summary, key...)
			summary = append(summary, terminator...)
			summary = append(summary, encoded_entry.Uint32toVarint(uint32(bytesPassed - len(key) - len(offset) - len(newline) - len(terminator)))...)
			summary = append(summary, newline...) 
			newlinesPassed = 0
		}
	}

	return summary
}
