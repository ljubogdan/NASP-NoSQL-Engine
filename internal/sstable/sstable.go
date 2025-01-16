package sstable

import (
	"NASP-NoSQL-Engine/internal/probabilistics"
	"NASP-NoSQL-Engine/internal/trees"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
)

const (
	ConfigPath = "../data/config.json"
)

type SSTable struct {
	SSTableName string // full putanja do foldera gde se nalazi sstable i ostali fajlovi, dobija se SSTablesPath + ime sstable-a + "/"
	// merkle pointer      // poredimo korene ako valajju dalje ako ne valjaju moramo tačno locirati položaj
	// bloom filter pointer
	DataName          string // BINARAN FAJL
	IndexName         string // BINARAN FAJL
	SummaryName       string // BINARAN FAJL - treba da bude u memoriji učitan
	MetadataName      string // block_size, no_blocks, merkle
	BloomFilterName   string // prvo njega pitamo da li je ključ u sstable-u, na disku se nalazi
	BlockSizeFileName string // veličina bloka u datom momentu
	MergeName         string // mode u kom se sstable nalazi, da li odvajamo strukture ili sve u jednom fajlu
	TOCName           string // tabela sadržaja svih fajlova koje imamo

	bloomfilter *probabilistics.BloomFilter
	metadata    *trees.MerkleTree

	// ako padne opcija da sve bude u SSTablePath, svi ostali su postavljeni na offsete u bajtovima "treba konvertovati" u uint64

	// za numeričke vrednosti se koristi varijabilni enkoding

	// nazivi svih fajlova su .bin ekstenzije
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
				sstableName := "sstable_00000"
				err := os.Mkdir(SSTablesPath+sstableName, 0755)
				HandleError(err, "Failed to create sstable folder")

				// pravimo sve fajlove unutar tog foldera
				CreateMergeFiles(sstableName)

			} else if len(folders) != 0 {
				// ako postoji folder onda nalazimo poslednji folder i povećavamo indeks za 1 po principu walFile := fmt.Sprintf("wal_%05d", i)
				lastFolder := folders[len(folders)-1].Name()
				lastFolderIndex, err := strconv.Atoi(lastFolder[8:])
				HandleError(err, "Failed to convert folder index to int")
				sstableName := "sstable_" + fmt.Sprintf("%05d", lastFolderIndex+1)
				err = os.Mkdir(SSTablesPath+sstableName, 0755)
				HandleError(err, "Failed to create sstable folder")

				// pravimo sve fajlove unutar tog foldera
				CreateMergeFiles(sstableName)
			}

			return &SSTable{
				SSTableName:       sstableName,
				DataName:          "",
				IndexName:         "",
				SummaryName:       "",
				MetadataName:      "",
				BloomFilterName:   "",
				BlockSizeFileName: "blocksize.bin",
				MergeName:         "merge.bin",
				TOCName:           "toc.bin",

				bloomfilter: probabilistics.NewBloomFilter(expectedElements, falsePositiveRate),
				metadata:    trees.NewMerkleTree(),
			}
		} else {

			folders, err := os.ReadDir(SSTablesPath)
			HandleError(err, "Failed to read sstables folder")
			fmt.Println(folders)

			sstableName := ""

			if len(folders) == 0 {
				fmt.Println("usao")
				sstableName := "sstable_00000"
				err := os.Mkdir(SSTablesPath+sstableName, 0755)
				HandleError(err, "Failed to create sstable folder")

				CreateStandardFiles(sstableName)

			} else if len(folders) != 0 {
				lastFolder := folders[len(folders)-1].Name()
				lastFolderIndex, err := strconv.Atoi(lastFolder[8:])
				HandleError(err, "Failed to convert folder index to int")
				sstableName := "sstable_" + fmt.Sprintf("%05d", lastFolderIndex+1)
				err = os.Mkdir(SSTablesPath+sstableName, 0755)
				HandleError(err, "Failed to create sstable folder")

				CreateStandardFiles(sstableName)
			}

			return &SSTable{
				SSTableName:       sstableName,
				DataName:          "data.bin",
				IndexName:         "index.bin",
				SummaryName:       "summary.bin",
				MetadataName:      "metadata.bin",
				BloomFilterName:   "bloomfilter.bin",
				BlockSizeFileName: "blocksize.bin",
				MergeName:         "merge.bin",
				TOCName:           "toc.bin",

				bloomfilter: probabilistics.NewBloomFilter(expectedElements, falsePositiveRate),
				metadata:    trees.NewMerkleTree(),
			}
		}
	}

	return nil
}

func CreateMergeFiles(sstableName string) {
	_, err := os.Create(SSTablesPath + sstableName + "/blocksize.bin")
	HandleError(err, "Failed to create blocksize file")
	_, err = os.Create(SSTablesPath + sstableName + "/merge.bin")
	HandleError(err, "Failed to create merge file")
	_, err = os.Create(SSTablesPath + sstableName + "/toc.bin")
	HandleError(err, "Failed to create toc file")
}

func CreateStandardFiles(sstableName string) {
	_, err := os.Create(SSTablesPath + sstableName + "/data.bin")
	HandleError(err, "Failed to create data file")
	_, err = os.Create(SSTablesPath + sstableName + "/index.bin")
	HandleError(err, "Failed to create index file")
	_, err = os.Create(SSTablesPath + sstableName + "/summary.bin")
	HandleError(err, "Failed to create summary file")
	_, err = os.Create(SSTablesPath + sstableName + "/metadata.bin")
	HandleError(err, "Failed to create metadata file")
	_, err = os.Create(SSTablesPath + sstableName + "/bloomfilter.bin")
	HandleError(err, "Failed to create bloomfilter file")
	_, err = os.Create(SSTablesPath + sstableName + "/blocksize.bin")
	HandleError(err, "Failed to create blocksize file")
	_, err = os.Create(SSTablesPath + sstableName + "/merge.bin")
	HandleError(err, "Failed to create merge file")
	_, err = os.Create(SSTablesPath + sstableName + "/toc.bin")
	HandleError(err, "Failed to create toc file")
}
