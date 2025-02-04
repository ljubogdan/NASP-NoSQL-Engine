package block_manager

import (
	"encoding/binary"
	"os"
)


func (bm *BlockManager) CreateMergeFiles(sstableName string) {
	_, err := os.Create(SSTablesPath + sstableName + "/data")
	HandleError(err, "Failed to create data file")
	_, err = os.Create(SSTablesPath + sstableName + "/blocksize")
	HandleError(err, "Failed to create blocksize file")
	_, err = os.Create(SSTablesPath + sstableName + "/merge")
	HandleError(err, "Failed to create merge file")
	_, err = os.Create(SSTablesPath + sstableName + "/toc")
	HandleError(err, "Failed to create toc file")
}

func (bm *BlockManager) CreateStandardFiles(sstableName string) {
	_, err := os.Create(SSTablesPath + sstableName + "/data")
	HandleError(err, "Failed to create data file")
	_, err = os.Create(SSTablesPath + sstableName + "/index")
	HandleError(err, "Failed to create index file")
	_, err = os.Create(SSTablesPath + sstableName + "/summary")
	HandleError(err, "Failed to create summary file")
	_, err = os.Create(SSTablesPath + sstableName + "/metadata")
	HandleError(err, "Failed to create metadata file")
	_, err = os.Create(SSTablesPath + sstableName + "/bloomfilter")
	HandleError(err, "Failed to create bloomfilter file")
	_, err = os.Create(SSTablesPath + sstableName + "/blocksize")
	HandleError(err, "Failed to create blocksize file")
	_, err = os.Create(SSTablesPath + sstableName + "/merge")
	HandleError(err, "Failed to create merge file")
	_, err = os.Create(SSTablesPath + sstableName + "/toc")
	HandleError(err, "Failed to create toc file")
}

// funkcija koja čita block size iz fajla na osnovu putanje
func (bm *BlockManager) ReadBlockSize(path string) uint32 {
	file, err := os.OpenFile(path, os.O_RDONLY, 0644)
	HandleError(err, "Failed to open block size file")

	var blockSize uint32
	err = binary.Read(file, binary.BigEndian, &blockSize)
	HandleError(err, "Failed to read block size from file")

	return blockSize
}

func (bm *BlockManager) WriteBlockSize(path string, blockSize uint32) {
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	HandleError(err, "Failed to open blocksize file")
	err = binary.Write(file, binary.BigEndian, blockSize)
	HandleError(err, "Failed to write block size to blocksize file")
	file.Close()
}

func (bm *BlockManager) WriteNONMergeMerge(path string) {
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	HandleError(err, "Failed to open merge file")
	_, err = file.Write([]byte("false"))
	HandleError(err, "Failed to write merge to merge file")
	file.Close()
}

func (bm *BlockManager) WriteNONMergeTOC(path string) {
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	HandleError(err, "Failed to open toc file")
	tocData := []string{"data", "index", "summary", "metadata", "bloomfilter", "blocksize", "merge"}
	for _, entry := range tocData {
		err = binary.Write(file, binary.BigEndian, uint32(len(entry)))
		HandleError(err, "Failed to write toc entry length to toc file")
		_, err = file.Write([]byte(entry))
		HandleError(err, "Failed to write toc entry to toc file")
	}
	file.Close()
}

func (bm *BlockManager) WriteMergeMerge(path string) {
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	HandleError(err, "Failed to open merge file")
	_, err = file.Write([]byte("true"))
	HandleError(err, "Failed to write merge to merge file")
	file.Close()
}

func (bm *BlockManager) WriteMergeTOC(path string) {
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	HandleError(err, "Failed to open toc file")
	tocData := []string{"data", "blocksize", "merge"}
	for _, entry := range tocData {
		err = binary.Write(file, binary.BigEndian, uint32(len(entry)))
		HandleError(err, "Failed to write toc entry length to toc file")
		_, err = file.Write([]byte(entry))
		HandleError(err, "Failed to write toc entry to toc file")
	}
	file.Close()
}