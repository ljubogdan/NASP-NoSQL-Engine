package sstable

import (
	"NASP-NoSQL-Engine/internal/config"
	"NASP-NoSQL-Engine/internal/entry"
	"NASP-NoSQL-Engine/internal/probabilistics"
	"NASP-NoSQL-Engine/internal/trees"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

const (
	ConfigPath = "../data/config.json"
)

type IndexTuple struct {
	Key             []byte
	PositionInBlock uint32
	BlockIndex      uint32
}

type SSTable struct {
	SSTableName       string
	DataName          string
	IndexName         string
	SummaryName       string
	MetadataName      string
	BloomFilterName   string
	BlockSizeFileName string
	MergeName         string
	TOCName           string

	BloomFilter *probabilistics.BloomFilter
	Metadata    *trees.MerkleTree

	BlockSize uint32
	Merge     bool
}

func NewEmptySSTable() *SSTable {
	merge := config.ReadMerge()
	expectedElements := config.ReadBloomFilterExpectedElements()
	falsePositiveRate := config.ReadBloomFilterFalsePositiveRate()

	sstableName := generateSSTableName()
	createSSTableFolder(sstableName)

	if merge {
		createMergeFiles(sstableName)
	} else {
		createStandardFiles(sstableName)
	}

	blockSize := config.ReadBlockSize()
	writeBlockSize(sstableName, blockSize)
	writeMergeStatus(sstableName, merge)
	writeTOC(sstableName, merge)

	return &SSTable{
		SSTableName:       sstableName,
		DataName:          "data",
		IndexName:         "index",
		SummaryName:       "summary",
		MetadataName:      "metadata",
		BloomFilterName:   "bloomfilter",
		BlockSizeFileName: "blocksize",
		MergeName:         "merge",
		TOCName:           "toc",

		BloomFilter: probabilistics.NewBloomFilter(expectedElements, falsePositiveRate),
		Metadata:    trees.NewMerkleTree(),

		BlockSize: blockSize,
		Merge:     merge,
	}
}

func (s *SSTable) WriteEntries(entries []entry.Entry) error {
	dataFile, err := os.OpenFile(filepath.Join(SSTablesPath, s.SSTableName, s.DataName), os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open data file: %v", err)
	}
	defer dataFile.Close()

	for _, e := range entries {
		if err := binary.Write(dataFile, binary.BigEndian, uint64(len(e.Value))); err != nil {
			return fmt.Errorf("failed to write entry size: %v", err)
		}
		if _, err := dataFile.Write(e.Value); err != nil {
			return fmt.Errorf("failed to write entry data: %v", err)
		}
		s.BloomFilter.Add(string(e.Key))
		s.Metadata.Insert(e.Value)
	}
	return nil
}

func (s *SSTable) ReadAllEntries() ([]entry.Entry, error) {
	dataFile, err := os.Open(filepath.Join(SSTablesPath, s.SSTableName, s.DataName))
	if err != nil {
		return nil, fmt.Errorf("failed to open data file: %v", err)
	}
	defer dataFile.Close()

	var entries []entry.Entry
	for {
		var size uint64
		if err := binary.Read(dataFile, binary.BigEndian, &size); err != nil {
			break
		}
		buffer := make([]byte, size)
		if _, err := dataFile.Read(buffer); err != nil {
			return nil, fmt.Errorf("failed to read entry data: %v", err)
		}
		entries = append(entries, entry.Entry{Value: buffer})
	}
	return entries, nil
}

func (s *SSTable) Delete() error {
	return os.RemoveAll(filepath.Join(SSTablesPath, s.SSTableName))
}

func generateSSTableName() string {
	folders, err := os.ReadDir(SSTablesPath)
	HandleError(err, "Failed to read sstables folder")

	if len(folders) == 0 {
		return "sstable_00000"
	}

	lastFolder := folders[len(folders)-1].Name()
	lastFolderIndex, err := strconv.Atoi(lastFolder[8:])
	HandleError(err, "Failed to convert folder index to int")
	return "sstable_" + fmt.Sprintf("%05d", lastFolderIndex+1)
}

func createSSTableFolder(sstableName string) {
	err := os.Mkdir(filepath.Join(SSTablesPath, sstableName), 0755)
	HandleError(err, "Failed to create sstable folder")
}

func writeBlockSize(sstableName string, blockSize uint32) {
	file, err := os.OpenFile(filepath.Join(SSTablesPath, sstableName, "blocksize"), os.O_RDWR, 0644)
	HandleError(err, "Failed to open blocksize file")
	defer file.Close()

	err = binary.Write(file, binary.BigEndian, blockSize)
	HandleError(err, "Failed to write block size to blocksize file")
}

func writeMergeStatus(sstableName string, merge bool) {
	file, err := os.OpenFile(filepath.Join(SSTablesPath, sstableName, "merge"), os.O_RDWR, 0644)
	HandleError(err, "Failed to open merge file")
	defer file.Close()

	status := "false"
	if merge {
		status = "true"
	}
	_, err = file.Write([]byte(status))
	HandleError(err, "Failed to write merge status to merge file")
}

func writeTOC(sstableName string, merge bool) {
	file, err := os.OpenFile(filepath.Join(SSTablesPath, sstableName, "toc"), os.O_RDWR, 0644)
	HandleError(err, "Failed to open toc file")
	defer file.Close()

	tocData := []string{"data", "blocksize", "merge"}
	if !merge {
		tocData = append(tocData, "index", "summary", "metadata", "bloomfilter")
	}

	for _, entry := range tocData {
		err = binary.Write(file, binary.BigEndian, uint32(len(entry)))
		HandleError(err, "Failed to write toc entry length to toc file")
		_, err = file.Write([]byte(entry))
		HandleError(err, "Failed to write toc entry to toc file")
	}
}

func createMergeFiles(sstableName string) {
	_, err := os.Create(filepath.Join(SSTablesPath, sstableName, "data"))
	HandleError(err, "Failed to create data file")
	_, err = os.Create(filepath.Join(SSTablesPath, sstableName, "blocksize"))
	HandleError(err, "Failed to create blocksize file")
	_, err = os.Create(filepath.Join(SSTablesPath, sstableName, "merge"))
	HandleError(err, "Failed to create merge file")
	_, err = os.Create(filepath.Join(SSTablesPath, sstableName, "toc"))
	HandleError(err, "Failed to create toc file")
}

func createStandardFiles(sstableName string) {
	_, err := os.Create(filepath.Join(SSTablesPath, sstableName, "data"))
	HandleError(err, "Failed to create data file")
	_, err = os.Create(filepath.Join(SSTablesPath, sstableName, "index"))
	HandleError(err, "Failed to create index file")
	_, err = os.Create(filepath.Join(SSTablesPath, sstableName, "summary"))
	HandleError(err, "Failed to create summary file")
	_, err = os.Create(filepath.Join(SSTablesPath, sstableName, "metadata"))
	HandleError(err, "Failed to create metadata file")
	_, err = os.Create(filepath.Join(SSTablesPath, sstableName, "bloomfilter"))
	HandleError(err, "Failed to create bloomfilter file")
	_, err = os.Create(filepath.Join(SSTablesPath, sstableName, "blocksize"))
	HandleError(err, "Failed to create blocksize file")
	_, err = os.Create(filepath.Join(SSTablesPath, sstableName, "merge"))
	HandleError(err, "Failed to create merge file")
	_, err = os.Create(filepath.Join(SSTablesPath, sstableName, "toc"))
	HandleError(err, "Failed to create toc file")
}

func ReadBlockSizeFromFile(path string) uint32 {
	file, err := os.OpenFile(path, os.O_RDONLY, 0644)
	HandleError(err, "Failed to open block size file")

	var blockSize uint32
	err = binary.Read(file, binary.BigEndian, &blockSize)
	HandleError(err, "Failed to read block size from file")

	return blockSize
}

func CreateNONMergeIndex(indexTuples []IndexTuple, blockSize uint32) []byte {
	index := make([]byte, 0)

	for _, it := range indexTuples {
		offset := it.BlockIndex * blockSize
		offset += it.PositionInBlock

		index = append(index, []byte(it.Key)...)
		index = append(index, 0)
		index = append(index, entry.Uint32ToBytes(offset)...)
		index = append(index, 10)
	}

	return index
}

func CreateNONMergeSummary(indexTuples []IndexTuple) []byte {
	summary := make([]byte, 0)

	thinning := config.ReadSummaryThinning()

	keys := make([]string, 0)
	for _, it := range indexTuples {
		keys = append(keys, string(it.Key))
	}

	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[i] > keys[j] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}

	summary = append(summary, []byte(keys[0])...)
	summary = append(summary, 0)
	summary = append(summary, []byte(keys[len(keys)-1])...)
	summary = append(summary, 10)

	for i := 0; i < len(indexTuples); i += int(thinning) {
		summary = append(summary, []byte(indexTuples[i].Key)...)
		summary = append(summary, 0)
		summary = append(summary, entry.Uint32ToBytes(indexTuples[i].PositionInBlock)...)
		summary = append(summary, 10)
	}

	return summary
}
