package sstable

import (
	// importovati merkle stablo
	// importovvati bloom filter
)

type SSTable struct {
	SSTablePath string  // full putanja
	// merkle pointer      // poredimo korene ako valajju dalje ako ne valjaju moramo tačno locirati položaj
	// bloom filter pointer
	DataPath string	// BINARAN FAJL
	IndexPath string     // BINARAN FAJL
	SummaryPath string    // BINARAN FAJL
	MetadataPath string      // block_size, no_blocks, merkle
	BloomFilterPath string   

	// ako padne opcija da sve bude u SSTablePath, svi ostali su postavljeni na offsete u bajtovima "treba konvertovati" u uint64 

	// za numeričke vrednosti se koristi varijabilni enkoding
}