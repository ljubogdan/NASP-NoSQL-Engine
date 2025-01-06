package entry

import (
	"encoding/binary"
	"hash/crc32"
)

const (
	CRC_SIZE        = 4
	TIMESTAMP_SIZE  = 8
	TOMBSTONE_SIZE  = 1
	TYPE_SIZE       = 1 // FULL : 1, FIRST : 2, MIDDLE : 3, LAST : 4
	KEY_SIZE_SIZE   = 8
	VALUE_SIZE_SIZE = 8

	CRC_START        = 0
	TIMESTAMP_START  = CRC_START + CRC_SIZE
	TOMBSTONE_START  = TIMESTAMP_START + TIMESTAMP_SIZE
	TYPE_START       = TOMBSTONE_START + TOMBSTONE_SIZE
	KEY_SIZE_START   = TYPE_START + TYPE_SIZE
	VALUE_SIZE_START = KEY_SIZE_START + KEY_SIZE_SIZE
	KEY_START        = VALUE_SIZE_START + VALUE_SIZE_SIZE
)

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

func BytesToUint32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

func Uint32ToBytes(i uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, i)
	return b
}

func BytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func Uint64ToBytes(i uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, i)
	return b
}

type Entry struct {
	CRC       uint32
	Timestamp uint64
	Tombstone byte
	Type      byte
	KeySize   uint64
	ValueSize uint64
	Key       string
	Value     []byte
}

// metoda koja konstruiše entry na osnovu niza bajtova
func ConstructEntry(data []byte) Entry {
	// ako dužina nije dovoljna za entry, vraćamo prazan entry
	if len(data) < int(CRC_SIZE+TIMESTAMP_SIZE+TOMBSTONE_SIZE+TYPE_SIZE+KEY_SIZE_SIZE+VALUE_SIZE_SIZE) {
		return Entry{}
	}

	crc := BytesToUint32(data[CRC_START : CRC_START+CRC_SIZE])
	timestamp := BytesToUint64(data[TIMESTAMP_START : TIMESTAMP_START+TIMESTAMP_SIZE])
	tombstone := data[TOMBSTONE_START]
	entryType := data[TYPE_START]
	keySize := BytesToUint64(data[KEY_SIZE_START : KEY_SIZE_START+KEY_SIZE_SIZE])
	valueSize := BytesToUint64(data[VALUE_SIZE_START : VALUE_SIZE_START+VALUE_SIZE_SIZE])
	key := string(data[KEY_START : KEY_START+keySize])
	value := data[KEY_START+keySize : KEY_START+keySize+valueSize]

	// provera CRC
	if crc != CRC32([]byte(key+string(value))) {
		panic("CRC check failed")
	}

	return Entry{
		CRC:       crc,
		Timestamp: timestamp,
		Tombstone: tombstone,
		Type:      entryType,
		KeySize:   keySize,
		ValueSize: valueSize,
		Key:       key,
		Value:     value,
	}
}
