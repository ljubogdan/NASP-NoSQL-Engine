package sstable

import (
	"NASP-NoSQL-Engine/internal/entry"
	"log"
	"sort"
	"sync"
)

const (
	SSTablesPath = "../data/sstables/"
)

func HandleError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %v", msg, err)
	}
}

type SSTableManager struct {
	Capacity uint32
	List     []*SSTable
	mu       sync.Mutex
}

func NewSSTableManager(capacity uint32) *SSTableManager {
	return &SSTableManager{
		Capacity: capacity,
		List:     make([]*SSTable, 0),
	}
}

func (manager *SSTableManager) AddSSTable(sstable *SSTable) {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	if uint32(len(manager.List)) == manager.Capacity {
		manager.List = manager.List[1:]
	}
	manager.List = append(manager.List, sstable)
}

func (manager *SSTableManager) Get(filename string) *SSTable {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	for _, sstable := range manager.List {
		if sstable.SSTableName == filename {
			return sstable
		}
	}
	return nil
}

func (manager *SSTableManager) FindSSTableForKey(key string) *SSTable {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	for _, sstable := range manager.List {
		if sstable.BloomFilter.Contains(key) {
			return sstable
		}
	}
	return nil
}

func (manager *SSTableManager) CompactSSTables() {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	if len(manager.List) < 2 {
		return
	}

	sort.Slice(manager.List, func(i, j int) bool {
		return manager.List[i].SSTableName < manager.List[j].SSTableName
	})

	var mergedEntries []entry.Entry
	for _, sstable := range manager.List {
		entries, err := sstable.ReadAllEntries()
		if err != nil {
			log.Printf("Failed to read entries from SSTable %s: %v", sstable.SSTableName, err)
			continue
		}
		mergedEntries = append(mergedEntries, entries...)
	}

	newSSTable := NewEmptySSTable()
	if err := newSSTable.WriteEntries(mergedEntries); err != nil {
		log.Printf("Failed to write entries to new SSTable: %v", err)
		return
	}

	for _, sstable := range manager.List {
		if err := sstable.Delete(); err != nil {
			log.Printf("Failed to delete SSTable %s: %v", sstable.SSTableName, err)
		}
	}

	manager.List = []*SSTable{newSSTable}
}
