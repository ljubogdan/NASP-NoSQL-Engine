package sstable

import (
	"NASP-NoSQL-Engine/internal/probabilistics"
	"NASP-NoSQL-Engine/internal/trees"
	"NASP-NoSQL-Engine/internal/config"
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

