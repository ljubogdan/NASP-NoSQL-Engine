package block_manager

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

const (
	ConfigPath = "../data/config.json"
	WalsPath   = "../data/wals/"
)

type BlockManager struct {
	BufferPool *BufferPool
	CachePool  *CachePool
}

func HandleError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %v", msg, err)
	}
}

func NewBlockManager() *BlockManager {
	return &BlockManager{
		BufferPool: NewBufferPool(),
		CachePool:  NewCachePool(),
	}
}

// ovo je metoda koja će na startu sistema napuniti buffer pool blokovima
// prolazimo kroz najskorašnjiji WAL fajl i učitavamo blokove u buffer pool

func (bm *BlockManager) FillBufferPool(walPath string) { // walFile je već kreiran samo ga prosledimo

	walFile := filepath.Base(walPath)

	// pročitamo iz konfiguracije system -> page_size -> default i system -> pages_per_block -> default
	// i pomnožimo ih da dobijemo veličinu bloka
	// ======= UPOZORENE: ako je došlo do izmene u configu, moramo sve WAL-ove pre toga flushovati =======

	data, err := os.ReadFile(ConfigPath)
	HandleError(err, "Failed to read config file")

	var config map[string]interface{}
	json.Unmarshal(data, &config)

	pageSize := uint32(config["SYSTEM"].(map[string]interface{})["page_size"].(map[string]interface{})["default"].(float64))
	pagesPerBlock := uint32(config["SYSTEM"].(map[string]interface{})["pages_per_block"].(map[string]interface{})["default"].(float64))

	blockSize := pageSize * pagesPerBlock
	fmt.Println(blockSize)

	file, err := os.Open(walPath)
	HandleError(err, "Failed to open WAL file")
	defer file.Close()

	fileInfo, err := file.Stat() // proveravamo veličinu fajla
	HandleError(err, "Failed to get file info")

	fileSize := uint32(fileInfo.Size())

	fullBlocks := fileSize / blockSize
	partialBlockSize := fileSize % blockSize
	partialBlocks := uint32(0)
	if partialBlockSize != 0 {
		partialBlocks = 1
	}

	counter := uint32(0)
	for counter < fullBlocks {
		bytes := make([]byte, blockSize)
		_, err := file.Read(bytes)
		HandleError(err, "Failed to read block from WAL file")
		block := NewBufferBlock(walFile, counter, bytes)
		bm.BufferPool.AddBlock(block)
		counter++
	}

	if partialBlocks == 1 {
		bytes := make([]byte, partialBlockSize)
		_, err := file.Read(bytes)
		HandleError(err, "Failed to read block from WAL file")

		zeroBytes := make([]byte, blockSize-partialBlockSize)
		bytes = append(bytes, zeroBytes...)

		block := NewBufferBlock(walFile, counter, bytes)
		bm.BufferPool.AddBlock(block)
		counter++
	}

	for counter < bm.BufferPool.Capacity {
		bytes := make([]byte, blockSize)
		block := NewBufferBlock(walFile, counter, bytes)
		bm.BufferPool.AddBlock(block)
		counter++
	}
}
