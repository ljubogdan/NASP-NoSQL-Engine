package api

import (
	"NASP-NoSQL-Engine/internal/block_manager"
	"NASP-NoSQL-Engine/internal/entry"
	"NASP-NoSQL-Engine/internal/memtable"
	"NASP-NoSQL-Engine/internal/sstable"
)

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
			continue
			// bogdan
		}
	}

	return entry.Entry{}, false
}
