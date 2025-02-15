package api

import (
	"NASP-NoSQL-Engine/internal/block_manager"
	"NASP-NoSQL-Engine/internal/entry"
	"NASP-NoSQL-Engine/internal/memtable"
	"NASP-NoSQL-Engine/internal/probabilistics"
	"NASP-NoSQL-Engine/internal/sstable"
	"encoding/binary"
	"fmt"
)

/*
const (
	SSTablesPath = "../data/sstables/"
)
*/

type ReadPath struct {
	BlockManager    *block_manager.BlockManager
	MemtableManager *memtable.MemtableManager
	SSTablesManager  *sstable.SSTableManager
}

func NewReadPath(blockManager *block_manager.BlockManager, memtableManager *memtable.MemtableManager, sstablesManager *sstable.SSTableManager) *ReadPath {
	return &ReadPath{
		BlockManager:    blockManager,
		MemtableManager: memtableManager,
		SSTablesManager:  sstablesManager,
	}
}

func (rpo *ReadPath) ReadEntry(key string) (entry.Entry, bool) {
	// za početak proveravamo da li entry postoji u memtable-u
	result, exists := rpo.MemtableManager.Find(key)
	if exists {
		return result, true
	}

	// ako ne postoji u memtable-u, proveravamo da li postoji u cache pool-u
	cacheEntry := rpo.BlockManager.CachePool.Get(key)
	if cacheEntry != nil {
		return entry.Entry{Key: key, Value: cacheEntry.Value}, true
	}

	// ako ne postoji ni u memtable-u ni u cache pool-u, proveravamo da li postoji u sstable-ima
	// prolazimo kroz svaku sstabelu i proveravamo prvo da li je sstabela mergeovana ili nije
	// najviša sstabela je najnovija, pa prvo proveravamo nju, idemo unazad dakle

	for i := len(rpo.SSTablesManager.List) - 1; i >= 0; i-- {
		sstable := rpo.SSTablesManager.List[i]
		// ako je merge:
		if sstable.Merge {
			continue
			// miljan
		} else {
			// za početak neophodno je proveriti u bloom filteru da li postoji entry sa zadatim ključem
			bloomFilter := rpo.FindNONMergeBloomFilter(sstable.SSTableName, sstable.BlockSize)

			if !bloomFilter.Contains([]byte(key)) { // ako ne postoji u bloom filteru, sigurno ne postoji ni u sstabeli
				fmt.Println("Entry with key", key, "does not exist")
				continue
			} else {
				fmt.Println("Entry with key", key, "may exist in sstable", sstable.SSTableName)
			}
		}
	}

	return entry.Entry{}, false
}

// funkcija koja preko block managera pronalazi non-merge blokove bloom filtera i deserijalizuje ih
func (rpo *ReadPath) FindNONMergeBloomFilter(sstableName string, blockSize uint32) *probabilistics.BloomFilter {
	// prvo kreiramo id prvog bloka
	blockID := "sstables-" + sstableName + "-bloomfilter"
	// pitamo block manager da li postoji blok sa zadatim id-em
	block := rpo.BlockManager.BufferPool.GetBlock(blockID, 0)
	if block == nil {
		// ako nema bloka, mora ga iskopati iz fajla
		block = rpo.BlockManager.ReadBlock(SSTablesPath + sstableName + "/" + "bloomfilter", 0, blockSize)
		rpo.BlockManager.BufferPool.AddBlock(block)
	}

	// prva 4 bajta su veličina bloom filtera
	bloomFilterSizeBytes := block.Data[:4] // <-- na osnovu ovoga i block size procenimo koliko nam treba ješ blokova da učitamo
	bloomFilterSize := binary.BigEndian.Uint32(bloomFilterSizeBytes)
	
	bytesToRead := bloomFilterSize // vremenom se umanjuje kako prolazimo kroz blokove
	bloomFilterBytes := make([]byte, 0)

	// pročitamo koliko možemo iz nultog bloka, ako se bytes to read smanji na 0, znači da smo pročitali sve (napomena: zadnji blok ima padding u obliku binarnih nula)
	// iteriramo kroz bajtove u nultom bloku
	for i := 4; i < len(block.Data); i++ {
		bloomFilterBytes = append(bloomFilterBytes, block.Data[i])
		bytesToRead--
		if bytesToRead == 0 {
			break
		}
	}

	// ako nismo pročitali sve, moramo pročitati i iz ostalih blokova
	for i := 1; bytesToRead > 0; i++ {
		block = rpo.BlockManager.BufferPool.GetBlock(blockID, uint32(i))
		if block == nil {
			block = rpo.BlockManager.ReadBlock(SSTablesPath + sstableName + "/" + "bloomfilter", uint32(i), blockSize)
			rpo.BlockManager.BufferPool.AddBlock(block)
		}

		// iteriramo kroz bajtove u bloku
		for j := 0; j < len(block.Data); j++ {
			bloomFilterBytes = append(bloomFilterBytes, block.Data[j])
			bytesToRead--
			if bytesToRead == 0 {
				break
			}
		}
	}

	// deserijalizujemo bloom filter
	bloomFilter, err := probabilistics.DeserializeFromBytes_BF(bloomFilterBytes)
	HandleError(err, "Failed to deserialize bloom filter")

	return bloomFilter
}
