package memtable

import (
	"NASP-NoSQL-Engine/internal/entry"
)

// type EntryData struct {
// 	Key   string
// 	Value []byte
// }

type MemtableData interface {
	Insert(data entry.Entry)
	Get(key string) (entry.Entry, bool)
	GetAll() []entry.Entry
	Size() int // samo da znamo koliko trenutno ima elemenata
}

// napraviti da skip lista ima ove metode, treba ispraviti
