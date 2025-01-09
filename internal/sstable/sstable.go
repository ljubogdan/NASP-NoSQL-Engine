package sstable

import (
	// importovati merkle stablo
	// importovvati bloom filter
)

type SSTable struct {
	SSTablePath string  // full putanja
	// merkle pointer
	// bloom filter pointer
	// data
	DataPath string
	IndexPath string
	SummaryPath string
}