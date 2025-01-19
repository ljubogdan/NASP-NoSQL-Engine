package sstable

import (
	"NASP-NoSQL-Engine/internal/probabilistics"
	"NASP-NoSQL-Engine/internal/trees"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
)

const (
	ConfigPath = "../data/config.json"
)

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

	bloomfilter *probabilistics.BloomFilter
	metadata    *trees.MerkleTree

	BlockSize uint32        // bitno samo prilikom kreiranja sstable-a, kasnije ove podatke čitamo iz fajla
	Merge bool
}

// funkcija koja kreira novi prazan sstable i vraća pokazivač na njega
func NewEmptySSTable() *SSTable {
	// nalazimo unutar konfig fajla MERGE i vidimo da li je true ili false
	data, err := os.ReadFile(ConfigPath)
	HandleError(err, "Failed to read config file")

	var config map[string]interface{}
	json.Unmarshal(data, &config)

	mergeConfig, exists := config["SSTABLE"].(map[string]interface{})["MERGE"]

	// pročitamo za bloom filter expected_elements i false_positive_rate
	expectedElements := uint32(config["BLOOM_FILTER"].(map[string]interface{})["expected_elements"].(float64))
	falsePositiveRate := config["BLOOM_FILTER"].(map[string]interface{})["false_positive_rate"].(float64)

	if exists {
		merge := mergeConfig.(bool)
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
				CreateMergeFiles(sstableName)

			} else if len(folders) != 0 {
				// ako postoji folder onda nalazimo poslednji folder i povećavamo indeks za 1 po principu walFile := fmt.Sprintf("wal_%05d", i)
				lastFolder := folders[len(folders)-1].Name()
				lastFolderIndex, err := strconv.Atoi(lastFolder[8:])
				HandleError(err, "Failed to convert folder index to int")
				sstableName = "sstable_" + fmt.Sprintf("%05d", lastFolderIndex+1)
				err = os.Mkdir(SSTablesPath+sstableName, 0755)
				HandleError(err, "Failed to create sstable folder")

				// pravimo sve fajlove unutar tog foldera
				CreateMergeFiles(sstableName)
			}

			// u block size fajlu upisujemo sistemsku vrednost block_size
			// u merge bin upisujemo 1 ako je merge true, 0 ako je false
			// u toc bin upisujemo sve fajlove koje imamo u sstable-u, u ovom slučaju to su data, blocksize, merge

			blockSize := uint32(config["SYSTEM"].(map[string]interface{})["block_size"].(map[string]interface{})["default"].(float64))
			file, err := os.OpenFile(SSTablesPath+sstableName+"/blocksize", os.O_RDWR, 0644)
			HandleError(err, "Failed to open blocksize file")
			err = binary.Write(file, binary.BigEndian, blockSize)
			HandleError(err, "Failed to write block size to blocksize file")
			file.Close()

			file, err = os.OpenFile(SSTablesPath+sstableName+"/merge", os.O_RDWR, 0644)
			HandleError(err, "Failed to open merge file")
			mergeValue := uint8(1)
			err = binary.Write(file, binary.BigEndian, mergeValue)
			HandleError(err, "Failed to write merge to merge file")
			file.Close()

			file, err = os.OpenFile(SSTablesPath+sstableName+"/toc", os.O_RDWR, 0644)
			HandleError(err, "Failed to open toc file")
			tocData := []string{"data", "blocksize", "merge"}
			for _, entry := range tocData {
				err = binary.Write(file, binary.BigEndian, uint32(len(entry)))
				HandleError(err, "Failed to write toc entry length to toc file")
				_, err = file.Write([]byte(entry))
				HandleError(err, "Failed to write toc entry to toc file")
			}
			file.Close()

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

				bloomfilter: probabilistics.NewBloomFilter(expectedElements, falsePositiveRate),
				metadata:    trees.NewMerkleTree(),

				BlockSize: blockSize,
				Merge: true,
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

				CreateStandardFiles(sstableName)

			} else if len(folders) != 0 {
				lastFolder := folders[len(folders)-1].Name()
				lastFolderIndex, err := strconv.Atoi(lastFolder[8:])
				HandleError(err, "Failed to convert folder index to int")
				sstableName = "sstable_" + fmt.Sprintf("%05d", lastFolderIndex+1)
				err = os.Mkdir(SSTablesPath+sstableName, 0755)
				HandleError(err, "Failed to create sstable folder")

				CreateStandardFiles(sstableName)
			}

			blockSize := uint32(config["SYSTEM"].(map[string]interface{})["block_size"].(map[string]interface{})["default"].(float64))
			file, err := os.OpenFile(SSTablesPath+sstableName+"/blocksize", os.O_RDWR, 0644)
			HandleError(err, "Failed to open blocksize file")
			err = binary.Write(file, binary.BigEndian, blockSize)
			HandleError(err, "Failed to write block size to blocksize file")
			file.Close()

			file, err = os.OpenFile(SSTablesPath+sstableName+"/toc", os.O_RDWR, 0644)
			HandleError(err, "Failed to open toc file")
			tocData := []string{"data", "index", "summary", "metadata", "bloomfilter", "blocksize", "merge"}
			for _, entry := range tocData {
				err = binary.Write(file, binary.BigEndian, uint32(len(entry)))
				HandleError(err, "Failed to write toc entry length to toc file")
				_, err = file.Write([]byte(entry))
				HandleError(err, "Failed to write toc entry to toc file")
			}
			file.Close()

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

				bloomfilter: probabilistics.NewBloomFilter(expectedElements, falsePositiveRate),
				metadata:    trees.NewMerkleTree(),

				BlockSize: blockSize,
				Merge: false,
			}
		}
	}

	return nil
}

func CreateMergeFiles(sstableName string) {
	_, err := os.Create(SSTablesPath + sstableName + "/data")
	HandleError(err, "Failed to create data file")
	_, err = os.Create(SSTablesPath + sstableName + "/blocksize")
	HandleError(err, "Failed to create blocksize file")
	_, err = os.Create(SSTablesPath + sstableName + "/merge")
	HandleError(err, "Failed to create merge file")
	_, err = os.Create(SSTablesPath + sstableName + "/toc")
	HandleError(err, "Failed to create toc file")
}

func CreateStandardFiles(sstableName string) {
	_, err := os.Create(SSTablesPath + sstableName + "/data")
	HandleError(err, "Failed to create data file")
	_, err = os.Create(SSTablesPath + sstableName + "/index")
	HandleError(err, "Failed to create index file")
	_, err = os.Create(SSTablesPath + sstableName + "/summary")
	HandleError(err, "Failed to create summary file")
	_, err = os.Create(SSTablesPath + sstableName + "/metadata")
	HandleError(err, "Failed to create metadata file")
	_, err = os.Create(SSTablesPath + sstableName + "/bloomfilter")
	HandleError(err, "Failed to create bloomfilter file")
	_, err = os.Create(SSTablesPath + sstableName + "/blocksize")
	HandleError(err, "Failed to create blocksize file")
	_, err = os.Create(SSTablesPath + sstableName + "/merge")
	HandleError(err, "Failed to create merge file")
	_, err = os.Create(SSTablesPath + sstableName + "/toc")
	HandleError(err, "Failed to create toc file")
}

// funkcija koja čita block size iz fajla na osnovu putanje
func ReadBlockSizeFromFile(path string) uint32 {
	file, err := os.OpenFile(path, os.O_RDONLY, 0644)
	HandleError(err, "Failed to open block size file")

	var blockSize uint32
	err = binary.Read(file, binary.BigEndian, &blockSize)
	HandleError(err, "Failed to read block size from file")

	return blockSize
}
