package wal

import (
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	"github.com/edsrzf/mmap-go"
)

const (
	CRC_SIZE        = 4
	TIMESTAMP_SIZE  = 8
	TOMBSTONE_SIZE  = 1
	KEY_SIZE_SIZE   = 8
	VALUE_SIZE_SIZE = 8

	CRC_START        = 0
	TIMESTAMP_START  = CRC_START + CRC_SIZE
	TOMBSTONE_START  = TIMESTAMP_START + TIMESTAMP_SIZE
	KEY_SIZE_START   = TOMBSTONE_START + TOMBSTONE_SIZE
	VALUE_SIZE_START = KEY_SIZE_START + KEY_SIZE_SIZE
	KEY_START        = VALUE_SIZE_START + VALUE_SIZE_SIZE
)

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

type WriteAheadLog struct {
	dir         string
	currentFile *os.File
	currentSize int64
	segmentID   int
	maxSize     int64
}

func NewWriteAheadLog(dir string, maxSize int64) (*WriteAheadLog, error) {
	wal := &WriteAheadLog{
		dir:     dir,
		maxSize: maxSize,
	}

	if err := wal.createNewSegment(); err != nil {
		return nil, fmt.Errorf("failed to initialize WAL: %v", err)
	}

	return wal, nil
}

func (wal *WriteAheadLog) WriteEntry(entry *Entry) error {
	// Serijalizacija zapisa
	data, err := SerializeEntry(entry)
	if err != nil {
		return fmt.Errorf("failed to serialize entry: %v", err)
	}

	// Provera veličine trenutnog segmenta
	if wal.currentSize+int64(len(data)) > wal.maxSize {
		if err := wal.createNewSegment(); err != nil {
			return fmt.Errorf("failed to create new segment: %v", err)
		}
	}

	_, err = wal.currentFile.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write entry to WAL: %v", err)
	}

	wal.currentSize += int64(len(data))
	return nil
}

func (wal *WriteAheadLog) createNewSegment() error {
	if wal.currentFile != nil {
		wal.currentFile.Close()
	}

	wal.segmentID++

	newFilePath := fmt.Sprintf("%s/%d.wal", wal.dir, wal.segmentID)
	file, err := os.Create(newFilePath)
	if err != nil {
		return fmt.Errorf("failed to create new segment: %v", err)
	}

	wal.currentFile = file
	wal.currentSize = 0

	return nil
}

func (wal *WriteAheadLog) ReadAllEntries() ([]*Entry, error) {
	var entries []*Entry

	files, err := filepath.Glob(fmt.Sprintf("%s/*.wal", wal.dir))
	if err != nil {
		return nil, fmt.Errorf("failed to list WAL files: %v", err)
	}

	// Svaki segment čitamo
	for _, filepath := range files {
		file, err := os.Open(filepath)
		if err != nil {
			return nil, fmt.Errorf("failed to open file: %v", err)
		}

		defer file.Close()

		mmapData, err := mmap.Map(file, mmap.RDONLY, 0)
		if err != nil {
			return nil, fmt.Errorf("failed to mmap segment %s: %v", filepath, err)
		}
		defer mmapData.Unmap()

		// Čitamo sve zapise iz segmenta
		offset := int64(0)
		fmt.Println(len(mmapData))
		for offset < int64(len(mmapData)) {
			entry, bytesRead, err := DeserializeFromOffset(mmapData, offset)
			if err != nil {
				return nil, fmt.Errorf("failed to deserialize entry: %v", err)
			}

			entries = append(entries, entry)
			offset += int64(bytesRead)

		}
	}

	return entries, nil
}

func DeserializeFromOffset(data []byte, offset int64) (*Entry, int, error) {

	if int(offset) >= len(data) {
		return nil, 0, fmt.Errorf("offset is out of bounds")
	}

	start := int(offset)

	if start+CRC_SIZE > len(data) {
		return nil, 0, fmt.Errorf("data is too short to deserialize (CRC)")
	}

	crc := bytesToUint32(data[start : start+CRC_SIZE])
	fmt.Println("CRC: ", crc)
	start += CRC_SIZE

	if start+TIMESTAMP_SIZE > len(data) {
		return nil, 0, fmt.Errorf("data is too short to deserialize (TIMESTAMP)")
	}

	timestamp := bytesToInt64(data[start : start+TIMESTAMP_SIZE])
	fmt.Println("TIMESTAMP: ", timestamp)
	start += TIMESTAMP_SIZE

	if start+TOMBSTONE_SIZE > len(data) {
		return nil, 0, fmt.Errorf("data is too short to deserialize (TOMBSTONE)")
	}

	tombstone := data[start] == 1
	fmt.Println("TOMBSTONE: ", tombstone)
	start += TOMBSTONE_SIZE

	if start+KEY_SIZE_SIZE > len(data) {
		return nil, 0, fmt.Errorf("data is too short to deserialize (KEY_SIZE)")
	}

	keySize := bytesToInt64(data[start : start+KEY_SIZE_SIZE])
	fmt.Println("KEY_SIZE: ", keySize)
	start += KEY_SIZE_SIZE

	if start+VALUE_SIZE_SIZE > len(data) {
		return nil, 0, fmt.Errorf("data is too short to deserialize (VALUE_SIZE)")
	}

	valueSize := bytesToInt64(data[start : start+VALUE_SIZE_SIZE])
	fmt.Println("VALUE_SIZE: ", valueSize)
	start += VALUE_SIZE_SIZE

	if start+int(keySize) > len(data) {
		return nil, 0, fmt.Errorf("data is too short to deserialize (KEY)")
	}

	key := string(data[start : start+int(keySize)])
	fmt.Println("KEY: ", key)
	start += int(keySize)

	if start+int(valueSize) > len(data) {
		return nil, 0, fmt.Errorf("data is too short to deserialize (VALUE)")
	}

	value := data[start : start+int(valueSize)]
	fmt.Println("VALUE: ", value)
	start += int(valueSize)

	computedCRC := CRC32(data[CRC_SIZE:start])

	if crc != computedCRC {
		return nil, 0, fmt.Errorf("CRC mismatch")
	}

	entry := &Entry{
		CRC:       crc,
		Timestamp: timestamp,
		Tombstone: tombstone,
		KeySize:   keySize,
		ValueSize: valueSize,
		Key:       key,
		Value:     value,
	}

	entrySize := start - int(offset)

	return entry, int(entrySize), nil
}
