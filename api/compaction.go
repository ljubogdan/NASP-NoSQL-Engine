package api

import (
	"NASP-NoSQL-Engine/internal/block_manager"
	"NASP-NoSQL-Engine/internal/config"
	"NASP-NoSQL-Engine/internal/encoded_entry"
	"NASP-NoSQL-Engine/internal/entry"
	"NASP-NoSQL-Engine/internal/sstable"
	"bytes"
	"encoding/binary"
)

type Compaction struct {
	readPath  *ReadPath
	writePath *WritePath
	method    string
	size      uint32
	max       uint32
}

func NewCompaction(rpo *ReadPath, wpo *WritePath) *Compaction {
	return &Compaction{readPath: rpo, writePath: wpo, method: config.ReadCompactionMethod(), size: config.ReadCompactionSize(), max: config.ReadCompactionMax()}
}

func (comp *Compaction) CheckForCompaction() (*[]sstable.SSTableIterator, uint16) {
	sstables := make([]*sstable.SSTable, 0)

	level := uint16(0)
	switch comp.method {
	default:
		fallthrough
	case "size_tired":
		for _, sstLevel := range comp.readPath.SSTablesManager.Levels {
			level++
			if len(sstLevel) >= int(comp.size) {
				sstables = sstLevel[:comp.size]
				break
			}
		}
	}

	return comp.readPath.GetStartingIteratorsForTables(&sstables), min(level, uint16(len(comp.readPath.SSTablesManager.Levels)-1))
}

func (comp *Compaction) Merge(iterators []sstable.SSTableIterator, level uint16) {
	sstEntries := make([]entry.Entry, len(iterators))
	for i := 0; i < len(sstEntries); i++ {
		if sstEntries[i].Key != iterators[i].LastKey {
			e, exists := comp.readPath.FindInDataByIterator(&iterators[i])
			sstEntries[i] = e
			if exists && sstEntries[i].Key <= (iterators)[i].LastKey {
				continue
			}
		}

		comp.writePath.SSTableManager.DeleteSSTable(iterators[i].SSTableName)
		sstEntries = append(sstEntries[0:i], sstEntries[i+1:]...)
		iterators = append(iterators[0:i], iterators[i+1:]...)
	}

	comp.writePath.BlockManager.ReadBidirectionalMapFromFile()

	sst := comp.writePath.SSTableManager.CreateSSTable()
	compression := sst.Compression
	merge := sst.Merge

	indexTuples := make([]sstable.IndexTuple, 0)

	filePath := "..-data-sstables-" + sst.SSTableName + "-data"
	blockFileId := "sstables-" + sst.SSTableName + "-data"
	currentBlockIndex := uint32(0)
	comp.writePath.BlockManager.BufferPool.AddBlock(block_manager.NewBufferBlock(blockFileId, currentBlockIndex, make([]byte, sst.BlockSize), sst.BlockSize, false))
	currentBlock := comp.writePath.BlockManager.BufferPool.GetBlock(blockFileId, currentBlockIndex)
	positionInBlock := uint32(0)
	if merge {
		positionInBlock = uint32(8)
	}

	for len(sstEntries) > 0 {
		min := ""
		minIndex := 0

		for i := 0; i < len(sstEntries); i++ {
			if min == "" || sstEntries[i].Key < min {
				min = sstEntries[i].Key
				minIndex = i
			} else if min == sstEntries[i].Key {
				if sstEntries[i].Key != iterators[i].LastKey {
					e, exists := comp.readPath.FindInDataByIterator(&iterators[i])
					sstEntries[i] = e
					if exists {
						continue
					}
				}

				comp.writePath.SSTableManager.DeleteSSTable(iterators[i].SSTableName)
				sstEntries = append(sstEntries[0:i], sstEntries[i+1:]...)
				iterators = append(iterators[0:i], iterators[i+1:]...)
				i--
			}
		}

		sst.BloomFilter.Add([]byte(sstEntries[minIndex].Key))

		e := encoded_entry.EncodeEntry(sstEntries[minIndex], comp.writePath.BlockManager.BidirectionalMap.GetByString(sstEntries[minIndex].Key), compression)
		compactValue := make([]byte, 0)
		compactValue = append(compactValue, e.Key...)
		compactValue = append(compactValue, e.Value...)

		indexTupleWritten := false // da li je index tuple upisan

		typeArray := make([][2]uint32, 0) // pamti pozicije TYPE elemenata

		compactBytesWritten := uint32(0)
		compactValueCurrentPosition := uint32(0)

		crcStart := uint32(0) // računamo start za svaki element jer se sada svaki put razlikuje u encoded entriju
		timestampStart := crcStart + uint32(len(e.CRC))
		tombstoneStart := timestampStart + uint32(len(e.Timestamp))
		typeStart := tombstoneStart + uint32(len(e.Tombstone))

		header := make([]byte, 0) // pravimo header koji će se uvek upisivati
		header = append(header, e.CRC...)
		header = append(header, e.Timestamp...)
		header = append(header, e.Tombstone...)
		header = append(header, e.Type...)
		header = append(header, e.KeySize...)
		header = append(header, e.ValueSize...)

		complete := false

		// izvršavamo dokle god ne bude complete, prave se novi blokovi i dodaju u buffer pool
		for !complete {
			// provera da li od trenutne pozicije u bloku ima dovoljno mesta za header
			if positionInBlock+uint32(len(header)) <= sst.BlockSize {

				if !indexTupleWritten {
					indexTuples = append(indexTuples, sstable.IndexTuple{Key: e.Key, PositionInBlock: positionInBlock, BlockIndex: currentBlockIndex})
					indexTupleWritten = true
				}

				for i := 0; i < len(header); i++ {
					currentBlock.Data[positionInBlock] = header[i]
					positionInBlock++

					if i == int(typeStart) {
						typeArray = append(typeArray, [2]uint32{currentBlockIndex, positionInBlock - 1})
					}
				}
			} else {
				sst.Metadata.AddBlock(&currentBlock.Data) // kada se blok napuni dodajemo ga u merkle stablo
				currentBlock.WrittenStatus = true
				comp.writePath.BlockManager.WriteBlock(filePath, currentBlock)

				// povećavamo indeks i kreiramo novi blok
				currentBlockIndex++
				currentBlock = block_manager.NewBufferBlock(blockFileId, currentBlockIndex, make([]byte, sst.BlockSize), sst.BlockSize, false)
				positionInBlock = 0
				continue
			}

			// upisujemo ključ i vrednost
			for i := positionInBlock; i < sst.BlockSize; i++ {
				currentBlock.Data[i] = compactValue[compactValueCurrentPosition]
				compactValueCurrentPosition++
				compactBytesWritten++
				positionInBlock++
				if compactBytesWritten == uint32(len(compactValue)) {
					complete = true
					break
				}
			}

			// ako smo završili sa upisom, onda postavljamo TYPE elemente
			if complete {
				if len(typeArray) == 1 {
					currentBlock.Data[typeArray[0][1]] = 1
				} else {
					currentBlock.Data[typeArray[len(typeArray)-1][1]] = 4
				}

			} else {
				if len(typeArray) == 1 {
					currentBlock.Data[typeArray[0][1]] = 2
				} else {
					currentBlock.Data[typeArray[len(typeArray)-1][1]] = 3
				}

				sst.Metadata.AddBlock(&currentBlock.Data)
				currentBlock.WrittenStatus = true
				comp.writePath.BlockManager.WriteBlock(filePath, currentBlock)

				currentBlockIndex++
				currentBlock = block_manager.NewBufferBlock(blockFileId, currentBlockIndex, make([]byte, sst.BlockSize), sst.BlockSize, false)
				positionInBlock = 0
			}
		}
		if sstEntries[minIndex].Key != iterators[minIndex].LastKey {
			e, exists := comp.readPath.FindInDataByIterator(&iterators[minIndex])
			sstEntries[minIndex] = e
			if exists {
				continue
			}
		}

		comp.writePath.SSTableManager.DeleteSSTable(iterators[minIndex].SSTableName)
		sstEntries = append(sstEntries[0:minIndex], sstEntries[minIndex+1:]...)
		iterators = append(iterators[0:minIndex], iterators[minIndex+1:]...)
	}

	// upisuje se poslednji blok ako nije već upisan (desi se ako nije skroz popunjen)
	if !currentBlock.WrittenStatus {
		sst.Metadata.AddBlock(&currentBlock.Data)
		comp.writePath.BlockManager.WriteNONMergeBlock(currentBlock)
		currentBlockIndex++
	}

	if merge {
		// upisuje se na kom bloku počinje bloom filter
		binary.BigEndian.PutUint16(comp.writePath.BlockManager.BufferPool.GetBlock(blockFileId, 0).Data[0:2], uint16(currentBlockIndex))
		currentBlock = block_manager.NewBufferBlock(blockFileId, currentBlockIndex, make([]byte, sst.BlockSize), sst.BlockSize, false)
		positionInBlock = 0

		var bfBuffer bytes.Buffer
		sst.BloomFilter.Serialize(&bfBuffer)
		bfData := bfBuffer.Bytes()
		for _, b := range bfData {
			for positionInBlock >= sst.BlockSize {
				currentBlock.WrittenStatus = true
				comp.writePath.BlockManager.WriteBlock(filePath, currentBlock)
				currentBlockIndex++
				positionInBlock -= sst.BlockSize
				currentBlock = block_manager.NewBufferBlock(blockFileId, currentBlockIndex, make([]byte, sst.BlockSize), sst.BlockSize, false)
			}

			currentBlock.Data[positionInBlock] = b
			positionInBlock++
		}

		// treba preći na sledeći blok nakon upisa bloom filtera
		comp.writePath.BlockManager.WriteBlock(filePath, currentBlock)
		currentBlockIndex++
		positionInBlock = 0
		currentBlock = block_manager.NewBufferBlock(blockFileId, currentBlockIndex, make([]byte, sst.BlockSize), sst.BlockSize, false)

		// dobavljamo 1. blok da upišemo na kom bloku počinje index
		currentBlock = comp.writePath.BlockManager.BufferPool.GetBlock(blockFileId, 0)
		binary.BigEndian.PutUint16(currentBlock.Data[2:4], uint16(currentBlockIndex))

		indexData := comp.writePath.SSTableManager.CreateNONMergeIndex(indexTuples, sst.BlockSize)
		summaryData := comp.writePath.SSTableManager.CreateNONMergeSummary(indexTuples, indexData, compression, currentBlockIndex*sst.BlockSize)

		comp.writePath.BlockManager.WriteBytesAsBlocks(*indexData, filePath, currentBlockIndex)
		currentBlockIndex += (uint32(len(*indexData)) + sst.BlockSize - 1) / sst.BlockSize // odlaže se povećanje indeksa da bi summary offset bio tačan
		binary.BigEndian.PutUint16(currentBlock.Data[4:6], uint16(currentBlockIndex))      // upisuje se na kom bloku počinje summary

		comp.writePath.BlockManager.WriteBytesAsBlocks(summaryData, filePath, currentBlockIndex)
		currentBlockIndex += (uint32(len(summaryData)) + sst.BlockSize - 1) / sst.BlockSize
		binary.BigEndian.PutUint16(currentBlock.Data[6:8], uint16(currentBlockIndex)) // upisuje se na kom bloku počinje merkle (metadata)

		sst.Metadata.Build() // nakon dodavanja svih data blokova radi se build za merkle stablo
		comp.writePath.BlockManager.WriteBytesAsBlocks(*sst.Metadata.Serialize(), filePath, currentBlockIndex)

		// upisujemo 1. blok ponovo sa sada zabeleženim podacima o početku svake sekcije
		comp.writePath.BlockManager.WriteBlock(filePath, currentBlock)
	} else {
		// kreiramo index i upisujemo ga u sstable
		index := comp.writePath.SSTableManager.CreateNONMergeIndex(indexTuples, sst.BlockSize) // znak pitanja da li treba da se pravi po blokovima ili sve odjednom...
		comp.writePath.BlockManager.WriteNONMergeIndex(*index, sst.SSTableName)

		// sada kreiramo summary
		summary := comp.writePath.SSTableManager.CreateNONMergeSummary(indexTuples, index, compression, 0)
		comp.writePath.BlockManager.WriteNONMergeSummary(summary, sst.SSTableName)

		// serijalizujemo bloom filter i upisujemo ga u sstable
		comp.writePath.BlockManager.WriteNONMergeBloomFilter(sst.BloomFilter, sst.SSTableName)

		sst.Metadata.Build() // nakon dodavanja svih data blokova radi se build za merkle stablo
		comp.writePath.BlockManager.WriteBytesAsBlocks(*sst.Metadata.Serialize(), SSTablesPath+sst.SSTableName+"/"+sst.MetadataName, 0)
	}

	// dodajemo sstable u listu svih sstabela
	sst.Metadata = nil
	sst.Level = level
	comp.writePath.BlockManager.WriteLevel(SSTablesPath+sst.SSTableName+"/level", level)
	comp.writePath.SSTableManager.AddSSTable(sst)
}
