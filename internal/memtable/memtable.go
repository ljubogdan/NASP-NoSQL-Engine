package memtable

import (
	"NASP-NoSQL-Engine/internal/entry"
	"NASP-NoSQL-Engine/internal/probabilistics"
	"NASP-NoSQL-Engine/internal/trees"
	"math"
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
	data      MemtableData
	max       uint16
	structure string
}

func NewMemtable(size uint16, structure string) *Memtable {
	if structure == "map" {
		return &Memtable{data: &MapWrapper{data: make(map[string]entry.Entry)}, max: size, structure: structure}
	} else if structure == "list" {
		return &Memtable{data: probabilistics.NewSkipList(int(math.Ceil(math.Log2(float64(size))))), max: size, structure: structure}
	} else {
		return &Memtable{data: trees.NewBTree(int(math.Ceil(math.Log2(float64(size))))), max: size, structure: structure}
	}
}

func (mt *Memtable) Put(key string, value []byte) {
	mt.data.Insert(entry.Entry{
		Key:       key,
		KeySize:   uint64(len(key)),
		Value:     value,
		ValueSize: uint64(len(value)),
		Tombstone: byte(0),
		Timestamp: uint64(time.Now().Unix()),
		Type:      byte(1),
		CRC:       uint32(entry.CRC32(append([]byte(key), value...))),
	})
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

func (mt *Memtable) GetAll() *[]entry.Entry {
	entries := mt.data.GetAll()
	sort.Slice(entries[:], func(i, j int) bool {
		return entries[i].Key < entries[j].Key
	})
	return &entries
}

func (mt *Memtable) Delete(key string) {
	value, exists := mt.data.Get(key)
	if exists {
		value.Tombstone = byte(1)
		value.ValueSize = 0
		value.Value = make([]byte, 0)
		mt.data.Insert(value)
	} else {
		mt.data.Insert(entry.Entry{
			Key:       key,
			KeySize:   uint64(len(key)),
			Value:     make([]byte, 0),
			ValueSize: 0,
			Tombstone: byte(1),
			Timestamp: uint64(time.Now().Unix()),
			CRC:       uint32(entry.CRC32(append([]byte(key), value.Value...))),
		})
	}
}

func (mt *Memtable) Full() bool {
	return mt.data.Size() >= int(mt.max)
}

func (mt *Memtable) Flush() *[]entry.Entry {
	entries := mt.GetAll()

	if mt.structure == "map" {
		mt.data = &MapWrapper{data: make(map[string]entry.Entry)}
	} else if mt.structure == "list" {
		mt.data = probabilistics.NewSkipList(int(math.Ceil(math.Log2(float64(mt.max)))))
	} else {
		mt.data = trees.NewBTree(int(math.Ceil(math.Log2(float64(mt.max)))))
	}

	// računamo za svaki entry CRC32
	// for i := 0; i < len(entries); i++ {
	// 	ent := &entries[i]
	// 	ent.CRC = uint32(entry.CRC32(append([]byte(ent.Key), ent.Value...)))
	// }

	return entries
}
