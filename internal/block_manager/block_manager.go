package block_manager

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
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
	blocksPerWal := uint32(config["WAL"].(map[string]interface{})["blocks_per_wal"].(float64))

	blockSize := pageSize * pagesPerBlock

	file, err := os.Open(walPath)
	HandleError(err, "Failed to open WAL file")
	defer file.Close()

	// prolazimo kroz wal fajl i učitavamo blokove u buffer pool
	// ako zafali za ceo blok popunimo ga nulama do kraja
	// kreiramo blokova koliko je blocksPerWal

	for i := uint32(0); i < blocksPerWal; i++ {
		data := make([]byte, blockSize)
		n, err := file.Read(data[:])
		if err != nil && err != io.EOF {
			HandleError(err, "Failed to read from WAL file")
		}

		if n < len(data) {
			for j := n; j < len(data); j++ {
				data[j] = 0
			}
		}

		bb := &BufferBlock{
			FileName: walFile,
			BlockNumber: i,
			Data: data,
		}

		bm.BufferPool.AddBlock(bb)
	}
}

func (bm *BlockManager) GetBlockFromBufferPool(index uint32) *BufferBlock {
	for e := bm.BufferPool.Pool.Front(); e != nil; e = e.Next() {
		bb := e.Value.(*BufferBlock)
		if bb.BlockNumber == index {
			return bb
		}
	}
	return nil
}

func (bm *BlockManager) WriteBufferPoolToWal(walPath string) string { // upisuje i pravi novi wal fajl
	file, err := os.OpenFile(walPath, os.O_RDWR, 0644)
	HandleError(err, "Failed to open WAL file")
	defer file.Close()

	for e := bm.BufferPool.Pool.Front(); e != nil; e = e.Next() {
		block := e.Value.(*BufferBlock)
		_, err := file.Write(block.Data)
		HandleError(err, "Failed to write block to WAL file")
	}

	err = file.Sync()	// stable write
	HandleError(err, "Failed to sync WAL file")

	bm.BufferPool.Clear()

	walFile := filepath.Base(walPath)

	num, _ := strconv.Atoi(walFile[4:])
	newWalFile := fmt.Sprintf("wal_%05d", num+1)

	_, err = os.Create(WalsPath + newWalFile)
	HandleError(err, "Failed to create new WAL file")

	bm.FillBufferPool(WalsPath + newWalFile)

	return WalsPath + newWalFile
}

func (bm *BlockManager) SyncBufferPoolToWal(walPath string) {
	file, err := os.OpenFile(walPath, os.O_RDWR, 0644)
	HandleError(err, "Failed to open WAL file")
	defer file.Close()

	for e := bm.BufferPool.Pool.Front(); e != nil; e = e.Next() {
		block := e.Value.(*BufferBlock)
		_, err := file.WriteAt(block.Data, int64(block.BlockNumber*uint32(len(block.Data))))
		HandleError(err, "Failed to write block to WAL file")
	}

	err = file.Sync()	// stable write
	HandleError(err, "Failed to sync WAL file")
}
