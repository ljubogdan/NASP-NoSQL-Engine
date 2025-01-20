package memtable

import (
	"NASP-NoSQL-Engine/internal/entry"
	"encoding/json"
	"log"
	"os"
)

const (
	ConfigPath = "../data/config.json"
)

type MemtableManager struct {
	tables  []Memtable
	current uint16
}

func HandleError(err error, msg string) {
	if err != nil {
		log.Printf("%s: %v", msg, err)
		// panic(msg + ": " + err.Error())
	}
	// if r := recover(); r != nil {
	// 	log.Print(r)
	// }
}

func NewMemtableManager() *MemtableManager {
	tableCount := uint16(5)
	tableSize := uint16(10)
	structure := "map"

	data, err := os.ReadFile(ConfigPath)
	HandleError(err, "Failed to read config file")

	var config map[string]interface{}
	json.Unmarshal(data, &config)

	memtableConfig, exists := config["MEMTABLE"]
	if exists {
		if count, exists := memtableConfig.(map[string]interface{})["number_of_tables"].(float64); exists {
			tableCount = uint16(count)
		}
		if count, exists := memtableConfig.(map[string]interface{})["entries_per_table"].(float64); exists {
			tableSize = uint16(count)
		}
	}

	tables := make([]Memtable, tableCount)
	for i := 0; i < int(tableCount); i++ {
		tables[i] = *NewMemtable(tableSize, structure)
	}
	return &MemtableManager{tables: tables, current: 0}
}

func (manager *MemtableManager) Next() *[]entry.Entry {
	manager.current = (manager.current + 1) % uint16(len(manager.tables))
	return manager.tables[(manager.current+1)%uint16(len(manager.tables))].Flush()
}

func (manager *MemtableManager) Insert(key string, value []byte) *[]entry.Entry {
	manager.tables[manager.current].Put(key, value)
	if manager.tables[manager.current].Full() {
		return manager.Next()
	}
	temp := (make([]entry.Entry, 0))
	return &temp
}

func (manager *MemtableManager) InsertFromWAL(newEntry *entry.Entry) *[]entry.Entry {
	manager.tables[manager.current].PutFromWAL(newEntry)
	if manager.tables[manager.current].Full() {
		return manager.Next()
	}
	temp := (make([]entry.Entry, 0))
	return &temp
}

func (manager *MemtableManager) Find(key string) (entry.Entry, bool) {
	tableCount := uint16(len(manager.tables))
	entry, exists := manager.tables[manager.current].Get(key)
	for i := (manager.current - 1 + tableCount) % tableCount; i != manager.current; i = (i - 1 + tableCount) % tableCount {
		if exists || entry.Tombstone == byte(1) {
			return entry, exists
		}
		entry, exists = manager.tables[i].Get(key)
	}
	return entry, exists
}

func (manager *MemtableManager) Delete(key string) *[]entry.Entry {
	manager.tables[manager.current].Delete(key)
	if manager.tables[manager.current].Full() {
		return manager.Next()
	}
	temp := (make([]entry.Entry, 0))
	return &temp
}
