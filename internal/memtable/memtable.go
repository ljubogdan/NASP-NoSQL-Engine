package memtable

type Memtable struct {
	data map[string][]byte
}

func NewMemtable() *Memtable {
	return &Memtable{data: make(map[string][]byte)}
}

func (mt *Memtable) Put(key string, value []byte) {
	mt.data[key] = value
}

func (mt *Memtable) Get(key string) ([]byte, bool) {
	value, exists := mt.data[key]
	return value, exists
}

func (mt *Memtable) Delete(key string) {
	delete(mt.data, key)
}
