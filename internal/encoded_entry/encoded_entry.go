package encoded_entry

import (
	"NASP-NoSQL-Engine/internal/entry"
	"encoding/binary"
	"fmt"
)

type EncodedEntry struct { // entry koji nastaje varint enkodovanjem običnog entry-ja
	CRC       []byte
	Timestamp []byte
	Tombstone []byte
	Type      []byte
	KeySize   []byte
	ValueSize []byte
	Key       []byte
	Value     []byte
}

func NewEncodedEntry(crc []byte, timestamp []byte, tombstone []byte, entryType []byte, keySize []byte, valueSize []byte, key []byte, value []byte) *EncodedEntry {
	return &EncodedEntry{
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

// funkcija koja enkodira entry u encoded entry, vraća enkodirani entry
func EncodeEntry(e entry.Entry, globalValue uint32, compression bool) EncodedEntry {
	encodedEntry := EncodedEntry{}

	encodedEntry.CRC = Uint32toVarint(e.CRC)
	encodedEntry.Timestamp = Uint64toVarint(e.Timestamp)
	encodedEntry.Tombstone = []byte{e.Tombstone}
	encodedEntry.Type = []byte{e.Type}

	// globalValue nam je sada key, treba da ga enkodiramo
	if compression {
		encodedEntry.Key = Uint32toVarint(globalValue)
	} else {
		encodedEntry.Key = []byte(e.Key)
	}

	encodedEntry.KeySize = Uint64toVarint(uint64(len(encodedEntry.Key)))

	// ako je tombsotne == 1, ovda valeSize i value je prazan niz bajtova
	if e.Tombstone == byte(0) {
		encodedEntry.ValueSize = Uint64toVarint(e.ValueSize)
 		encodedEntry.Value = e.Value
	} else {
		encodedEntry.ValueSize = []byte{} // tehnički ne postoji value size
		encodedEntry.Value = []byte{}
	}

	return encodedEntry
}

// uint64 u varint
func Uint64toVarint(value uint64) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, value)
	return buf[:n]
}

// varint u uint64
func VarintToUint64(buf []byte) (uint64, error) {
	value, n := binary.Uvarint(buf)
	if n <= 0 {
		return 0, fmt.Errorf("UvarintToUint64 failed")
	}
	return value, nil
}

// uint32 u varint
func Uint32toVarint(value uint32) []byte {
	return Uint64toVarint(uint64(value))
}

// varint u uint32
func VarintToUint32(buf []byte) (uint32, error) {
	value, err := VarintToUint64(buf)
	if err != nil {
		return 0, err
	}
	if value > uint64(^uint32(0)) {
		return 0, fmt.Errorf("VarintToUint32 failed - value too large")
	}
	return uint32(value), nil
}

// funkcija koja prima niz bajtova i od njegovog početka čita varint
// vraća pročitane bajtove
func ReadVarint(buf []byte) []byte {
	_, n := binary.Uvarint(buf)
	if n <= 0 {
		return nil
	}
	return buf[:n]
}


