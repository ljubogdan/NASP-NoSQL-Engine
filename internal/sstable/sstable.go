package sstable

import (
	"NASP-NoSQL-Engine/internal/probabilistics"
	"NASP-NoSQL-Engine/internal/trees"
	"NASP-NoSQL-Engine/internal/config"
	"NASP-NoSQL-Engine/internal/entry"
	"fmt"
	"os"
	"strconv"
)

// funkcija koja kreira novi prazan sstable i vraća pokazivač na njega
func (sstm *SSTableManager) CreateSSTable() *SSTable {

	merge := config.ReadMerge()

	expectedElements := config.ReadBloomFilterExpectedElements()
	falsePositiveRate := config.ReadBloomFilterFalsePositiveRate()

	if merge {
		// moramo napraviti folder unutar SSTablesPath-a u fomatu sstable_xxxxx, indeksi se povećavaju za 1, ako nema ni jedan sstable onda je to sstable_00000
		// u tom folderu pravimo sve fajlove

		// prvo učitavamo spisak foldera u SSTablesPath-u
		folders, err := os.ReadDir(SSTablesPath)
		HandleError(err, "Failed to read sstables folder")

		sstableName := ""

		// ako ne postoji nijedan folder onda pravimo prvi folder sstable_00000
		if len(folders) == 0 {
			sstableName = "sstable_00000"
			err := os.Mkdir(SSTablesPath+sstableName, 0755)
			HandleError(err, "Failed to create sstable folder")

			// pravimo sve fajlove unutar tog foldera
			sstm.BlockManager.CreateMergeFiles(sstableName)

		} else if len(folders) != 0 {
			// ako postoji folder onda nalazimo poslednji folder i povećavamo indeks za 1 po principu walFile := fmt.Sprintf("wal_%05d", i)
			lastFolder := folders[len(folders)-1].Name()
			lastFolderIndex, err := strconv.Atoi(lastFolder[8:])
			HandleError(err, "Failed to convert folder index to int")
			sstableName = "sstable_" + fmt.Sprintf("%05d", lastFolderIndex+1)
			err = os.Mkdir(SSTablesPath+sstableName, 0755)
			HandleError(err, "Failed to create sstable folder")

			// pravimo sve fajlove unutar tog foldera
			sstm.BlockManager.CreateMergeFiles(sstableName)
		}

		// u block size fajlu upisujemo sistemsku vrednost block_size
		// u merge bin upisujemo 1 ako je merge true, 0 ako je false
		// u toc bin upisujemo sve fajlove koje imamo u sstable-u, u ovom slučaju to su data, blocksize, merge

		blockSize := config.ReadBlockSize()
		sstm.BlockManager.WriteBlockSize(SSTablesPath+sstableName+"/blocksize", blockSize)
		sstm.BlockManager.WriteMergeMerge(SSTablesPath+sstableName+"/merge")
		sstm.BlockManager.WriteMergeTOC(SSTablesPath+sstableName+"/toc")

		return &SSTable{
			SSTableName:       sstableName,
			DataName:          "data",
			IndexName:         "",
			SummaryName:       "",
			MetadataName:      "",
			BloomFilterName:   "",
			BlockSizeFileName: "blocksize",
			MergeName:         "merge",
			TOCName:           "toc",

			BloomFilter: probabilistics.NewBloomFilter(expectedElements, falsePositiveRate),
			Metadata:    trees.NewMerkleTree(),

			BlockSize: blockSize,
			Merge:     true,
		}
	} else {

		folders, err := os.ReadDir(SSTablesPath)
		HandleError(err, "Failed to read sstables folder")
		fmt.Println(folders)

		sstableName := ""

		if len(folders) == 0 {
			sstableName = "sstable_00000"
			err := os.Mkdir(SSTablesPath+sstableName, 0755)
			HandleError(err, "Failed to create sstable folder")

			sstm.BlockManager.CreateStandardFiles(sstableName)

		} else if len(folders) != 0 {
			lastFolder := folders[len(folders)-1].Name()
			lastFolderIndex, err := strconv.Atoi(lastFolder[8:])
			HandleError(err, "Failed to convert folder index to int")
			sstableName = "sstable_" + fmt.Sprintf("%05d", lastFolderIndex+1)
			err = os.Mkdir(SSTablesPath+sstableName, 0755)
			HandleError(err, "Failed to create sstable folder")

			sstm.BlockManager.CreateStandardFiles(sstableName)
		}

		blockSize := config.ReadBlockSize()
		sstm.BlockManager.WriteBlockSize(SSTablesPath+sstableName+"/blocksize", blockSize)
		sstm.BlockManager.WriteNONMergeMerge(SSTablesPath+sstableName+"/merge")
		sstm.BlockManager.WriteNONMergeTOC(SSTablesPath+sstableName+"/toc")

		return &SSTable{
			SSTableName:       sstableName,
			DataName:          "data",
			IndexName:         "index",
			SummaryName:       "summary",
			MetadataName:      "metadata",
			BloomFilterName:   "bloomfilter",
			BlockSizeFileName: "blocksize",
			MergeName:         "merge",
			TOCName:           "toc",

			BloomFilter: probabilistics.NewBloomFilter(expectedElements, falsePositiveRate),
			Metadata:    trees.NewMerkleTree(),

			BlockSize: blockSize,
			Merge:     false,
		}
	}
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
