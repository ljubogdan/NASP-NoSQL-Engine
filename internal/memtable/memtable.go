package memtable

import (
	"NASP-NoSQL-Engine/internal/entry"
	"sort"
	"time"
)

type MapWrapper struct {
	data map[string]entry.Entry
}

func (mw *MapWrapper) Insert(data entry.Entry) {
	mw.data[data.Key] = data
}

func (mw *MapWrapper) Delete(key string) {
	delete(mw.data, key)
}

func (mw *MapWrapper) Get(key string) (entry.Entry, bool) {
	value, exists := mw.data[key]
	return value, exists
}

func (mw *MapWrapper) GetAll() []entry.Entry {
	entries := make([]entry.Entry, 0)
	for _, value := range mw.data {
		entries = append(entries, value)
	}
	return entries
}

func (mw *MapWrapper) Size() int {
	return len(mw.data)
}

type Memtable struct {
	data MemtableData
	max  uint16
}

func NewMemtable(size uint16) *Memtable {
	return &Memtable{data: &MapWrapper{data: make(map[string]entry.Entry)}, max: size}
}

func (mt *Memtable) Put(key string, value []byte) {
	mt.data.Insert(entry.Entry{Key: key, KeySize: uint64(len(key)), Value: value, ValueSize: uint64(len(value)), Tombstone: byte(0), Timestamp: uint64(time.Now().Unix())})
}

func (mt *Memtable) PutFromWAL(newEntry *entry.Entry) {
	mt.data.Insert(*newEntry)
}

func (mt *Memtable) Get(key string) (entry.Entry, bool) {
	value, exists := mt.data.Get(key)
	if value.Tombstone == byte(1) {
		exists = false
	}
	return value, exists
}

func (mt *Memtable) Delete(key string) {
	value, exists := mt.data.Get(key)
	if exists {
		value.Tombstone = byte(1)
		mt.data.Insert(value)
	} else {
		mt.data.Insert(entry.Entry{Key: key, KeySize: uint64(len(key)), Value: make([]byte, 0), ValueSize: 0, Tombstone: byte(1), Timestamp: uint64(time.Now().Unix())})
	}
}

func (mt *Memtable) Full() bool {
	return mt.data.Size() >= int(mt.max)
}

func (mt *Memtable) Flush() *[]entry.Entry {
	entries := mt.data.GetAll()
	sort.Slice(entries[:], func(i, j int) bool {
		return entries[i].Key < entries[j].Key
	})
	mt.data = &MapWrapper{data: make(map[string]entry.Entry)}

	// računamo za svaki entry CRC32
	for i := 0; i < len(entries); i++ {
		ent := &entries[i]
		ent.CRC = uint32(entry.CRC32(append([]byte(ent.Key), ent.Value...)))
	}

	return &entries
}
