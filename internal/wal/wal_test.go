package wal

import (
	//"fmt"
	"os"
	"testing"
	"time"
)

// go test -v ./internal/wal -run TestWAL

func TestWAL(t *testing.T) {
	dir := "./wal_test"
	err := os.Mkdir(dir, 0755)
	if err != nil && !os.IsExist(err) {
		t.Fatalf("failed to create test directory: %v", err)
	}
	defer os.RemoveAll(dir)

	wal, err := NewWriteAheadLog(dir, 1024)
	if err != nil {
		t.Fatalf("failed to create new WAL: %v", err)
	}

	entry1 := &Entry{
		Timestamp: time.Now().Unix(),
		Tombstone: false,
		Key:       "hello",
		Value:     []byte("world"),
		KeySize:   int64(len("hello")),
		ValueSize: int64(len("world")),
	}

	entry2 := &Entry{
		Timestamp: time.Now().Unix(),
		Tombstone: false,
		Key:       "foo",
		Value:     []byte("bar"),
		KeySize:   int64(len("foo")),
		ValueSize: int64(len("bar")),
	}
	

	t.Logf("Writing Entry1: Key=%s, Value=%s", entry1.Key, string(entry1.Value))
	if err := wal.WriteEntry(entry1); err != nil {
		t.Fatalf("failed to write entry to WAL: %v", err)
	}

	t.Logf("Writing Entry2: Key=%s, Value=%s", entry2.Key, string(entry2.Value))
	if err := wal.WriteEntry(entry2); err != nil {
		t.Fatalf("failed to write entry to WAL: %v", err)
	}

	entries, err := wal.ReadAllEntries()
	if err != nil {
		t.Fatalf("failed to read all entries: %v", err)
	}

	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}

	readEntry1 := entries[0]
	t.Logf("Read Entry1: Key=%s, Value=%s", readEntry1.Key, string(readEntry1.Value))
	if readEntry1.Key != "hello" || string(readEntry1.Value) != "world" {
		t.Errorf("expected key 'hello' and value 'world', got key '%s' and value '%s'", readEntry1.Key, string(readEntry1.Value))
	}

	readEntry2 := entries[1]
	t.Logf("Read Entry2: Key=%s, Value=%s", readEntry2.Key, string(readEntry2.Value))
	if readEntry2.Key != "foo" || string(readEntry2.Value) != "bar" {
		t.Errorf("expected key 'foo' and value 'bar', got key '%s' and value '%s'", readEntry2.Key, string(readEntry2.Value))
	}

	t.Log("WAL test passed")
}
