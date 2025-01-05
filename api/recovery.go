package api

import (
	"NASP-NoSQL-Engine/internal/entry"
	"log"
	"os"
	//"path/filepath"
	"encoding/json"
)

const (
	ConfigPath = "NASP-NoSQL-Engine/data/config.json"
	WalsPath   = "NASP-NoSQL-Engine/data/wals/"
)

func HandleError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %v", msg, err)
	}
}

// metoda koja će na startu sistema vratiti entrije iz zadnjeg wala (listu)
func (wpo *WritePath) GetEntriesFromLastWal() []entry.Entry {
	// napomena: neophodno je nakon promene broja blokova po walu flushovati sve walove
	// jer se ova metoda oslanja na config fajl

	walPath := wpo.WalManager.Wal.Path

	data, err := os.ReadFile(ConfigPath)
	HandleError(err, "Failed to read config file")

	var config map[string]interface{}
	json.Unmarshal(data, &config)

	pageSize := uint32(config["SYSTEM"].(map[string]interface{})["page_size"].(map[string]interface{})["default"].(float64))
	pagesPerBlock := uint32(config["SYSTEM"].(map[string]interface{})["pages_per_block"].(map[string]interface{})["default"].(float64))

	blockSize := pageSize * pagesPerBlock

	file, err := os.Open(walPath)
	HandleError(err, "Failed to open WAL file")

	fileInfo, err := file.Stat()
	HandleError(err, "Failed to get file info")

	if fileInfo.Size()%int64(blockSize) != 0 {
		log.Fatalf("WAL file is corrupted")
	}

	blocksInWal := uint32(fileInfo.Size() / int64(blockSize))

	entries := make([]entry.Entry, 0)
	//entryData := make([]byte, 0)
	positionInBlock := uint32(0)

	for i := uint32(0); i < blocksInWal; i++ {
		// kada god složimo jedan entry, dodamo ga u listu
		// imaćemo slučajeve first, middle, last, full
		// kada naidjemo na fizički bajt koji je jednak 0, preskačemo i povećamo positionInBlock
		
		blockData := make([]byte, blockSize)
		_, err := file.ReadAt(blockData, int64(i*blockSize))
		HandleError(err, "Failed to read block data")

		for positionInBlock < blockSize {
			if blockData[positionInBlock] == 0 {
				positionInBlock++
				continue
			}
		// radi se na ovom delu koda

		}

	}

	return entries
}
