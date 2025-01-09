package memtable

import (
	"NASP-NoSQL-Engine/internal/entry"
	"fmt"
	"testing"
)

func TestMemtable(t *testing.T) {
	manager := NewMemtableManager()
	tableCount := len(manager.tables)
	tableSize := int(manager.tables[0].max)

	flushed := make([]entry.Entry, 0)
	flushed = append(flushed, *manager.Insert("1", make([]byte, 1))...)
	flushed = append(flushed, *manager.Insert("2", make([]byte, 2))...)
	flushed = append(flushed, *manager.Insert("4", make([]byte, 4))...)
	flushed = append(flushed, *manager.Insert("3", make([]byte, 3))...)
	flushed = append(flushed, *manager.Insert("5", make([]byte, 5))...)
	flushed = append(flushed, *manager.Insert("6", make([]byte, 6))...)
	flushed = append(flushed, *manager.Insert("7", make([]byte, 7))...)
	flushed = append(flushed, *manager.Insert("8", make([]byte, 8))...)
	flushed = append(flushed, *manager.Insert("9", make([]byte, 9))...)
	flushed = append(flushed, *manager.Insert("10", make([]byte, 10))...)
	flushed = append(flushed, *manager.Insert("11", make([]byte, 0))...)

	flushCount := (((11 + (tableSize - 1)) / tableSize) - tableCount + 1) * tableSize
	if (flushCount > 0 && len(flushed) != flushCount) || (flushCount <= 0 && len(flushed) != 0) {
		t.Errorf("Flush Memtable: Expected %d flushed entries, got %d", flushCount, len(flushed))
	}

	flushed = append(flushed, *manager.Insert("10", make([]byte, 2))...)
	update, exists := manager.Find("10")
	if !exists || update.Key != "10" || len(update.Value) != 2 {
		t.Errorf("Put Memtable: Expected key '10' and value [0, 0] after update, got %v", update)
	}

	get1, exists := manager.Find("2")
	if len(flushed) < 2 {
		if !exists || get1.Key != "2" || len(get1.Value) != 2 {
			t.Errorf("Get Memtable: Expected to get key '2' and value [0, 0], got %v", get1)
		}
	} else {
		if exists {
			t.Errorf("Get Memtable: Entry was supposed to be flushed, got %v", get1)
		}
	}
	get2, exists := manager.Find("11")
	if len(flushed) < 11 {
		if !exists || get2.Key != "11" || len(get2.Value) != 0 {
			t.Errorf("Get Memtable: Expected to get key '11' and value [], got %v", get2)
		}
	} else {
		if exists {
			t.Errorf("Get Memtable: Entry was supposed to be flushed, got %v", get2)
		}
	}
	get3, exists := manager.Find("asdafas")
	if exists {
		t.Errorf("Get Memtable: Entry was not supposed to exist, got %v", get3)
	}

	manager.Delete("2")
	del1, exists := manager.Find("2")
	if exists || del1.Tombstone != byte(1) {
		t.Errorf("Delete Memtable: Entry %v was not deleted correctly", del1)
	}
	manager.Delete("11")
	del2, exists := manager.Find("11")
	if exists || del2.Tombstone != byte(1) {
		t.Errorf("Delete Memtable: Entry %v was not deleted correctly", del2)
	}
	manager.Delete("asdafas")
	del3, exists := manager.Find("asdafas")
	if exists || del3.Tombstone != byte(1) {
		t.Errorf("Delete Memtable: Entry %v was not deleted correctly", del3)
	}

	for i := 0; i < len(manager.tables); i++ {
		fmt.Println(manager.tables[i].data.GetAll())
	}
}
