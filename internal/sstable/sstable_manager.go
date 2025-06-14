package sstable

import (
	"NASP-NoSQL-Engine/internal/block_manager"
	"NASP-NoSQL-Engine/internal/config"
	"NASP-NoSQL-Engine/internal/encoded_entry"
	"NASP-NoSQL-Engine/internal/probabilistics"
	"NASP-NoSQL-Engine/internal/trees"
	"log"
	"os"
)

const (
	SSTablesPath = "../data/sstables/"
)

type SSTableManager struct { // LSM sistem
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
	CompressionName   string
	TOCName           string

	BloomFilter *probabilistics.BloomFilter
	Metadata    *trees.MerkleTree

	BlockSize   uint32 // bitno samo prilikom kreiranja sstable-a, kasnije ove podatke čitamo iz fajla
	Merge       bool
	Compression bool
}

type SSTableIterator struct {
	SSTableName string
	Merge       bool
	Compression bool
	BlockSize   uint32
	Offset      uint32
	LastKey     string
}

func HandleError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %v", msg, err)
	}
}

func NewSSTableManager() *SSTableManager {
	return &SSTableManager{List: make([]*SSTable, 0), Capacity: 100000}
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
func (sstm *SSTableManager) CreateNONMergeSummary(indexTuples []IndexTuple, index *[]byte, compression bool, startingOffset uint32) []byte {
	summary := make([]byte, 0)

	// čitamo kolika je proredjenost (e.g. 5 znači svaki 5. IndexTuple uzimamo)
	thinning := config.ReadSummaryThinning()

	// za početak prodjemo kroz listu ključeva i vidimo koji ključ je najmanji i koji je najveći, to treba upisati na početku summary-a
	keys := make([]string, 0)
	for _, it := range indexTuples {
		if compression {
			keyUint32, err := encoded_entry.VarintToUint32(it.Key)
			HandleError(err, "Error converting varint to uint32")
			keys = append(keys, sstm.BlockManager.BidirectionalMap.GetByUint32(keyUint32))
		} else {
			keys = append(keys, string(it.Key))
		}
	}

	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[i] > keys[j] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}

	// upisujemo najmanji i najveći ključ
	if compression {
		summary = append(summary, encoded_entry.Uint32toVarint(sstm.BlockManager.BidirectionalMap.GetByString(keys[0]))...)
		summary = append(summary, 0)
		summary = append(summary, encoded_entry.Uint32toVarint(sstm.BlockManager.BidirectionalMap.GetByString(keys[len(keys)-1]))...)
		summary = append(summary, 10)
	} else {
		summary = append(summary, []byte(keys[0])...)
		summary = append(summary, 0)
		summary = append(summary, []byte(keys[len(keys)-1])...)
		summary = append(summary, 10)
	}

	if compression {
		newlinesPassed := thinning - 1

		// sada prilagodjavamo algoritam za varint
		// kada čitamo terminatorni bajt ima 0 na najvišem bitu
		// izgled indexa kroz koji se prolazi je na osnovu povratne vrednosti funkcije CreateNONMergeIndex

		bytesPassed := 0
		for bytesPassed < len(*index) {

			key := encoded_entry.ReadVarint((*index)[bytesPassed:])
			bytesPassed += len(key)

			// preskačemo terminatorni bajt
			terminator := encoded_entry.ReadVarint((*index)[bytesPassed:])
			bytesPassed += len(terminator)

			// čitamo offset
			offset := encoded_entry.ReadVarint((*index)[bytesPassed:])
			bytesPassed += len(offset)

			// preskačemo newline karakter
			newline := encoded_entry.ReadVarint((*index)[bytesPassed:])
			bytesPassed += len(newline)

			newlinesPassed++

			if newlinesPassed == thinning { // jer smo već pročitali ključ i offset...
				summary = append(summary, key...)
				summary = append(summary, terminator...)
				summary = append(summary, encoded_entry.Uint32toVarint(uint32(bytesPassed-len(key)-len(offset)-len(newline)-len(terminator))+startingOffset)...)
				summary = append(summary, newline...)
				newlinesPassed = 0
			}
		}
	} else {
		// za početak odmah upisujemo prvi ključ i offset 0
		summary = append(summary, []byte(keys[0])...)
		summary = append(summary, 0)
		summary = append(summary, encoded_entry.Uint32toVarint(startingOffset)...)
		summary = append(summary, 10)

		currentKeyIndex := 0
		newlinesPassed := 0

		for bytesPassed := 0; bytesPassed < len(*index); bytesPassed++ {
			if (*index)[bytesPassed] == 10 { // ako je null bajt
				newlinesPassed++
				currentKeyIndex++
			}

			if newlinesPassed == int(thinning) && currentKeyIndex < len(keys) {
				summary = append(summary, []byte(keys[currentKeyIndex])...)
				summary = append(summary, 0)
				summary = append(summary, encoded_entry.Uint32toVarint(uint32(bytesPassed+1)+startingOffset)...) // brojimo bajtove od 0 zato je plus 1
				summary = append(summary, 10)
				newlinesPassed = 0
			}
		}
	}

	return summary
}

// funkcija koja na početku programa prolazi kroz sstable folder i učitava sve sstable-ove u memoriju
// odnosno kreira sstable objekte i dodaje ih u listu sstable-ova
func (sstm *SSTableManager) LinkSSTable(sstableName string, dataName string, summaryName string, indexName string, metadataName string, bloomFilterName string, blockSizeFileName string, mergeName string, compressionName string, tocName string) {
	// pre nego što linkujemo moramo da pročitamo sve podatke iz fajlova sem bloom filtera, merkle stabla
	// toc, indexa, summarija i data
	// učitavamo block size, merge, compression
	blockSize := sstm.BlockManager.ReadBlockSize(SSTablesPath + sstableName + "/" + blockSizeFileName)
	merge := sstm.BlockManager.ReadMerge(SSTablesPath + sstableName + "/" + mergeName)
	compression := sstm.BlockManager.ReadCompression(SSTablesPath + sstableName + "/" + compressionName)

	// kreiramo sstable objekat
	sstable := &SSTable{
		SSTableName:       sstableName,
		DataName:          dataName,
		IndexName:         indexName,
		SummaryName:       summaryName,
		MetadataName:      metadataName,
		BloomFilterName:   bloomFilterName,
		BlockSizeFileName: blockSizeFileName,
		MergeName:         mergeName,
		CompressionName:   compressionName,
		TOCName:           tocName,

		BlockSize:   blockSize,
		Merge:       merge,
		Compression: compression,

		BloomFilter: nil,
		Metadata:    nil,
	}

	// dodajemo sstable u listu sstable-ova
	sstm.AddSSTable(sstable)
}

// funkcija koja na početku programa ulitava i sortira sve sstabele u memoriju pojedinačno sa link funkcijom
func (sstm *SSTableManager) LoadSSTables() {
	// učitavamo sve foldere iz main sstables foldera
	folders, err := os.ReadDir(SSTablesPath)
	HandleError(err, "Failed to read sstables folder")

	// prolazimo kroz sve foldere i linkujemo ih
	for _, folder := range folders {
		// učitavamo sve fajlove iz foldera
		files, err := os.ReadDir(SSTablesPath + folder.Name())
		HandleError(err, "Failed to read sstable folder")

		// prolazimo kroz sve fajlove i linkujemo ih
		var dataName, summaryName, indexName, metadataName, bloomFilterName, blockSizeFileName, mergeName, compressionName, tocName string
		for _, file := range files {
			switch file.Name() {
			case "data":
				dataName = file.Name()
			case "summary":
				summaryName = file.Name()
			case "index":
				indexName = file.Name()
			case "metadata":
				metadataName = file.Name()
			case "bloomfilter":
				bloomFilterName = file.Name()
			case "blocksize":
				blockSizeFileName = file.Name()
			case "merge":
				mergeName = file.Name()
			case "compression":
				compressionName = file.Name()
			case "toc":
				tocName = file.Name()
			}
		}

		// linkujemo sstable
		sstm.LinkSSTable(folder.Name(), dataName, summaryName, indexName, metadataName, bloomFilterName, blockSizeFileName, mergeName, compressionName, tocName)

		// za svaki slučaj sortiramo od najmanjeg do najvećeg sstabele objekte prema sstable name
		// npr. sstable_00001, sstable_00002, sstable_00003...

		for i := 0; i < len(sstm.List); i++ {
			for j := i + 1; j < len(sstm.List); j++ {
				if sstm.List[i].SSTableName > sstm.List[j].SSTableName {
					sstm.List[i], sstm.List[j] = sstm.List[j], sstm.List[i]
				}
			}
		}
	}
}
