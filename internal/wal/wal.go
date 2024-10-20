package wal

import (
	"fmt"
	"os"
)

// neki test

type WriteAheadLog struct {
	file *os.File
}

func NewWriteAheadLog(path string) (*WriteAheadLog, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &WriteAheadLog{file: file}, nil
}

func (wal *WriteAheadLog) WriteEntry(key string, value []byte) error {
	entry := fmt.Sprintf("%s=%s\n", key, value)
	_, err := wal.file.WriteString(entry)
	return err
}

func (wal *WriteAheadLog) Close() error {
	return wal.file.Close()
}
