package api

import (
	"NASP-NoSQL-Engine/internal/block_manager"
	"NASP-NoSQL-Engine/internal/encoded_entry"
	"NASP-NoSQL-Engine/internal/entry"
	"NASP-NoSQL-Engine/internal/memtable"
	"NASP-NoSQL-Engine/internal/probabilistics"
	"NASP-NoSQL-Engine/internal/sstable"
	"encoding/binary"
	"fmt"
)

/*
const (
	SSTablesPath = "../data/sstables/"
)
*/

type ReadPath struct {
	BlockManager    *block_manager.BlockManager
	MemtableManager *memtable.MemtableManager
	SSTablesManager *sstable.SSTableManager
}

func NewReadPath(blockManager *block_manager.BlockManager, memtableManager *memtable.MemtableManager, sstablesManager *sstable.SSTableManager) *ReadPath {
	return &ReadPath{
		BlockManager:    blockManager,
		MemtableManager: memtableManager,
		SSTablesManager: sstablesManager,
	}
}

// ============ SUMMARY I POTREBNE STRUKTURE ============
type Summary struct {
	MinKey []byte // mora ovako da ostane jer ne znamo kako izgleda ključ, da li je bilo kompresije ili ne
	MaxKey []byte

	MinKeyVarint uint32
	MaxKeyVarint uint32

	KeysOffsets        []KeyOffset
	KeysOffsetsVarints []KeyOffsetVarint
}

type KeyOffset struct {
	Key    []byte
	Offset uint32
}

type KeyOffsetVarint struct {
	Key    uint32
	Offset uint32
}

// ======================================================

func StripPadding(data []byte) []byte {
	i := len(data) - 1
	for ; i > -1 && data[i] == 0; i-- {
	}
	return data[:i+1]
}

func (rpo *ReadPath) ReadEntry(key string) (entry.Entry, bool) {

	rpo.BlockManager.ReadBidirectionalMapFromFile() // priprema bidirekcione mape za rad

	// za početak proveravamo da li entry postoji u memtable-u
	result, exists := rpo.MemtableManager.Find(key)
	if exists {
		return result, true
	}

	// ako ne postoji u memtable-u, proveravamo da li postoji u cache pool-u
	cacheEntry := rpo.BlockManager.CachePool.Get(key)
	if cacheEntry != nil {
		return entry.Entry{Key: key, Value: cacheEntry.Value}, true
	}

	// ako ne postoji ni u memtable-u ni u cache pool-u, proveravamo da li postoji u sstable-ima
	// prolazimo kroz svaku sstabelu i proveravamo prvo da li je sstabela mergeovana ili nije
	// najviša sstabela je najnovija, pa prvo proveravamo nju, idemo unazad dakle

	for i := len(rpo.SSTablesManager.List) - 1; i >= 0; i-- {
		sstable := rpo.SSTablesManager.List[i]
		folderPath := SSTablesPath + sstable.SSTableName + "/"
		blockSize := rpo.BlockManager.ReadBlockSize(folderPath + sstable.BlockSizeFileName)
		// ako je merge:
		if sstable.Merge {
			dataPath := folderPath + sstable.DataName
			block := rpo.BlockManager.ReadBlock(dataPath, 0, blockSize)

			sectionIndexed := make([]uint8, 4)
			for i := 0; i < 4; i++ {
				sectionIndexed[i] = uint8(block.Data[i])
			}

			var bfData []byte
			for i := uint8(0); i < sectionIndexed[0]; i++ {
				bfData = append(bfData, rpo.BlockManager.ReadBlock(dataPath, uint32(i), blockSize).Data...)
			}
			bloomFilter, err := probabilistics.DeserializeFromBytes_BF(StripPadding(bfData[8:]))
			HandleError(err, "Failed to deserialize bloom filter")

			if !bloomFilter.Contains([]byte(key)) {
				continue
			}

			// 1. blok summary dela
			block = rpo.BlockManager.ReadBlock(dataPath, uint32(sectionIndexed[2]), blockSize)
			offsetInBlock := 0

			var minKey string
			var maxKey string
			lastOffset := uint64(blockSize) * uint64(sectionIndexed[1]) // pamti zadnji offset iz summary/index
			jumped := false                                             // prati da li smo na idex delu

			if sstable.Compression {
				minKeyBytes := encoded_entry.ReadVarint(block.Data[offsetInBlock:])
				minKeyVarint, err := encoded_entry.VarintToUint32(minKeyBytes)
				HandleError(err, "Unable to read min key in summary of "+sstable.DataName)
				minKey = rpo.BlockManager.BidirectionalMap.ReverseMap[minKeyVarint]
				offsetInBlock += len(minKeyBytes) + 1

				maxKeyBytes := encoded_entry.ReadVarint(block.Data[offsetInBlock:])
				maxKeyVarint, err := encoded_entry.VarintToUint32(maxKeyBytes)
				HandleError(err, "Unable to read max key in summary of "+sstable.DataName)
				maxKey = rpo.BlockManager.BidirectionalMap.ReverseMap[maxKeyVarint]
				offsetInBlock += len(minKeyBytes) + 1

				if key < minKey || key > maxKey {
					continue
				}

				for true {
					if offsetInBlock >= int(blockSize) {
						offsetInBlock -= int(blockSize)
						block = rpo.BlockManager.ReadBlock(dataPath, block.BlockNumber+1, blockSize)
					}

					nextKeyBytes, done := encoded_entry.ReadVarintBytes(block.Data[offsetInBlock:])
					fmt.Println(nextKeyBytes)
					offsetInBlock += len(nextKeyBytes) + 1
					for !done {
						offsetInBlock = 0
						block = rpo.BlockManager.ReadBlock(dataPath, block.BlockNumber+1, blockSize)
						fragment, end := encoded_entry.ReadVarintBytes(block.Data[offsetInBlock:])
						nextKeyBytes = append(nextKeyBytes, fragment...)
						done = end
						offsetInBlock = len(fragment) + 1
					}
					nextKeyVarint, err := encoded_entry.VarintToUint32(nextKeyBytes)
					// HandleError(err, "Faild to read key")
					if err != nil {
						nextKeyVarint = 0
					}
					nextKey := rpo.BlockManager.BidirectionalMap.ReverseMap[nextKeyVarint]

					nextOffsetBytes, done := encoded_entry.ReadVarintBytes(block.Data[offsetInBlock:])
					offsetInBlock += len(nextOffsetBytes) + 1
					for !done {
						offsetInBlock = 0
						block = rpo.BlockManager.ReadBlock(dataPath, block.BlockNumber+1, blockSize)
						fragment, end := encoded_entry.ReadVarintBytes(block.Data[offsetInBlock:])
						nextOffsetBytes = append(nextKeyBytes, fragment...)
						done = end
						offsetInBlock = len(fragment) + 1
					}
					nextOffset, err := encoded_entry.VarintToUint64(nextOffsetBytes)
					// HandleError(err, "Faild to read offset")
					if err != nil {
						nextOffset = 0
					}

					if nextKey > key || nextKey < minKey || block.BlockNumber >= uint32(sectionIndexed[3]) {
						if jumped {
							break
						}

						block = rpo.BlockManager.ReadBlock(dataPath, uint32(lastOffset/uint64(blockSize)), blockSize)
						offsetInBlock = int(lastOffset % uint64(blockSize))
						jumped = true
					} else {
						lastOffset = nextOffset
						minKey = nextKey // koristim minKeyVarint za pamćenje najbližeg ključa (čisto da ne pravim novu promenjivu)
					}
				}

			} else {
				minKeyBytes := ReadNullTerminatedString(block.Data[offsetInBlock:])
				minKey = string(minKeyBytes)
				offsetInBlock += len(minKeyBytes) + 1

				maxKeyBytes := ReadNewlineTerminatedString(block.Data[offsetInBlock:])
				maxKey = string(maxKeyBytes)
				HandleError(err, "Unable to read max key in summary of "+sstable.DataName)
				offsetInBlock += len(minKeyBytes) + 1

				if key < minKey || key > maxKey {
					continue
				}

				for true {
					if offsetInBlock >= int(blockSize) {
						offsetInBlock = 0
						block = rpo.BlockManager.ReadBlock(dataPath, block.BlockNumber+1, blockSize)
					}

					nextKeyBytes, done := ReadNullTerminatedStringBytes(block.Data[offsetInBlock:])
					offsetInBlock += len(nextKeyBytes) + 1
					for !done {
						offsetInBlock = 0
						block = rpo.BlockManager.ReadBlock(dataPath, block.BlockNumber+1, blockSize)
						fragment, end := ReadNullTerminatedStringBytes(block.Data[offsetInBlock:])
						nextKeyBytes = append(nextKeyBytes, fragment...)
						done = end
						offsetInBlock = len(fragment) + 1
					}
					nextKey := string(nextKeyBytes)

					nextOffsetBytes, done := encoded_entry.ReadVarintBytes(block.Data[offsetInBlock:])
					offsetInBlock += len(nextOffsetBytes) + 1
					for !done {
						offsetInBlock = 0
						block = rpo.BlockManager.ReadBlock(dataPath, block.BlockNumber+1, blockSize)
						fragment, end := encoded_entry.ReadVarintBytes(block.Data[offsetInBlock:])
						nextOffsetBytes = append(nextKeyBytes, fragment...)
						done = end
						offsetInBlock = len(fragment) + 1
					}
					nextOffset, err := encoded_entry.VarintToUint64(nextOffsetBytes)
					// HandleError(err, "Faild to read offset")
					if err != nil {
						nextOffset = 0
					}

					if nextKey > key || nextKey < minKey || block.BlockNumber >= uint32(sectionIndexed[3]) {
						if jumped {
							break
						}

						block = rpo.BlockManager.ReadBlock(dataPath, uint32(lastOffset/uint64(blockSize)), blockSize)
						offsetInBlock = int(lastOffset % uint64(blockSize))
						jumped = true
					} else {
						lastOffset = nextOffset
						minKey = nextKey // koristim minKeyVarint za pamćenje najbližeg ključa (čisto da ne pravim novu promenjivu)
					}
				}
			}

			// može se desiti da najbliži ključ nije onaj koji tražimo pa prelazimo na sledežći sstable
			if minKey == key {
				return rpo.FindInData(sstable.SSTableName, blockSize, uint32(lastOffset), sstable.Compression)
			}

		} else {
			// za početak neophodno je proveriti u bloom filteru da li postoji entry sa zadatim ključem
			bloomFilter := rpo.FindAndDeserializeNONMergeBF(sstable.SSTableName, sstable.BlockSize)

			if !bloomFilter.Contains([]byte(key)) { // ako ne postoji u bloom filteru, sigurno ne postoji ni u sstabeli
				continue
			} else {
				// slučaj da entry možda postoji u sstabeli
				// dakle neophodno je učitati summary u memoriju i proveriti da li se ključ nalazi u tom opsegu
				// prvo namtakodje treba informacija o tome da li se radi kompresija ili ne

				compression := sstable.Compression

				// učitavamo summary
				summary := rpo.FindAndDeserializeNONMergeSummary(sstable.SSTableName, sstable.BlockSize, compression)

				// printujemo kako izgleda summary ------------------------>>>>>> obrisati kasnije
				summary.Print(compression)

				// proveravamo da li se ključ nalazi u opsegu summarija (radimo sa string ili byte verzijom ključa)
				stringLowerBound, stringUpperBound := rpo.SetBounds(key, summary, compression)

				if key < stringLowerBound || key > stringUpperBound {
					continue // ključ nije u opsegu summarija, idemo na sledeću sstabelu
				} else {
					// funkcija koja koriguje Bounds za pretragu
					stringLowerBound = rpo.CorrectLowerBound(key, summary, compression)
					stringUpperBound = rpo.CorrectUpperBound(key, summary, compression)
					fmt.Println("Lower bound: ", stringLowerBound)
					fmt.Println("Upper bound: ", stringUpperBound)
				}

				// sada idemo u index strukturu i tražimo ključ
				found, offset := rpo.FindInIndex(sstable.SSTableName, sstable.BlockSize, key, stringLowerBound, stringUpperBound, summary, compression)

				fmt.Println("\nFound: ", found)
				fmt.Println("Offset: ", offset)
				fmt.Println()

				if found {
					// sada idemo u data strukturu i čitamo entry
					return rpo.FindInData(sstable.SSTableName, sstable.BlockSize, offset, compression)
				} else {
					continue
				}
			}
		}
	}

	return entry.Entry{}, false
}

// funkcija koja pronalazi entry u data strukturi
func (rpo *ReadPath) FindInData(sstableName string, blockSize uint32, offset uint32, compression bool) (entry.Entry, bool) {
	// sada pročitamo blok u kome se nalazi entry
	// prema offsetu procenimo koji je index bloka u pitanju
	blockIndex := offset / blockSize

	// pre svakog zapisa entrija unutar bloka imamo njegom header koji ima:
	/* CRC, Timestamp, Tombstone, Type, KeySize, ValueSize */
	// ako ne stane sve u blok onda se na početku sledećeg bloka opet nalazi header

	block := rpo.BlockManager.BufferPool.GetBlock("sstables-"+sstableName+"-data", blockIndex)
	if block == nil {
		block = rpo.BlockManager.ReadBlock(SSTablesPath+sstableName+"/"+"data", blockIndex, blockSize)
		rpo.BlockManager.BufferPool.AddBlock(block)
	}

	// oduzmemo od offseta celobrojni broj blokova
	toSubtract := blockIndex * blockSize

	// korigujemo offset
	correctedOffset := offset - toSubtract

	crcVarint := encoded_entry.ReadVarint(block.Data[correctedOffset:])
	correctedOffset += uint32(len(crcVarint))
	timestampVarint := encoded_entry.ReadVarint(block.Data[correctedOffset:])
	correctedOffset += uint32(len(timestampVarint))
	tombstoneVarint := encoded_entry.ReadVarint(block.Data[correctedOffset:])
	correctedOffset += uint32(len(tombstoneVarint))
	typeVarint := encoded_entry.ReadVarint(block.Data[correctedOffset:])
	correctedOffset += uint32(len(typeVarint))
	keySizeVarint := encoded_entry.ReadVarint(block.Data[correctedOffset:])
	correctedOffset += uint32(len(keySizeVarint))

	valueSizeVarint := []byte{}

	// ako je tombstone 1 onda nema value size i value
	if tombstoneVarint[0] != byte(1) {
		valueSizeVarint = encoded_entry.ReadVarint(block.Data[correctedOffset:])
		correctedOffset += uint32(len(valueSizeVarint))
	}

	keyAndValueBytes := make([]byte, 0) // ovde ćemo zalepiti ključ i vrednost, kasnije ćemo ih odvojiti
	toIterate := uint64(0)              // koliko puta treba iterirati

	// saberemo key size i value size i toliko puta iteriramo
	if len(valueSizeVarint) == 0 {
		keySize, err := encoded_entry.VarintToUint64(keySizeVarint)
		HandleError(err, "Failed to convert varint to uint64")
		toIterate = keySize
	} else {
		keySize, err := encoded_entry.VarintToUint64(keySizeVarint)
		HandleError(err, "Failed to convert varint to uint64")
		valueSize, err := encoded_entry.VarintToUint64(valueSizeVarint)
		HandleError(err, "Failed to convert varint to uint64")
		toIterate = keySize + valueSize
	}

	/*
		IDEJA
			Dakle iteriramo kroz blokove i spajamo bajtove ključa i vrednosti
			ako dodjemo do kraja bloka, čitamo sledeći blok i nastavljamo sa spajanjem
			ako dodjemo do kraja vrednosti, završavamo sa spajanjem
			kada se zapis prekine na početku sledećeg bloka imamo header opet
	*/

	complete := false

	for i := uint64(0); i < toIterate; {
		if complete {
			break
		}
		if correctedOffset >= blockSize {
			blockIndex++
			block = rpo.BlockManager.BufferPool.GetBlock("sstables-"+sstableName+"-data", blockIndex)
			if block == nil {
				block = rpo.BlockManager.ReadBlock(SSTablesPath+sstableName+"/"+"data", blockIndex, blockSize)
				rpo.BlockManager.BufferPool.AddBlock(block)
			}
			correctedOffset = 0

			// pomerimo corrected offset za CRC, Timestamp, Tombstone, Type, KeySize, ValueSize odnosno header
			correctedOffset += (uint32(len(crcVarint)) + uint32(len(timestampVarint)) + uint32(len(tombstoneVarint)) + uint32(len(typeVarint)) + uint32(len(keySizeVarint)) + uint32(len(valueSizeVarint)))
		}

		// pročitamo bajtove ključa i vrednosti
		keyAndValueBytes = append(keyAndValueBytes, block.Data[correctedOffset])
		correctedOffset++
		i++

		// proverimo da li smo završili sa spajanjem
		if i >= toIterate {
			complete = true
		}
	}

	if complete {
		keySize, err := encoded_entry.VarintToUint32(keySizeVarint)
		HandleError(err, "Failed to convert varint to uint64")
		keyBytes := keyAndValueBytes[:keySize]
		valueBytes := keyAndValueBytes[keySize:]

		if compression {
			keyUint32, err := encoded_entry.VarintToUint32(keyBytes)
			HandleError(err, "Failed to convert varint to uint32")
			key := rpo.BlockManager.BidirectionalMap.GetByUint32(keyUint32)
			return entry.Entry{Key: key, Value: valueBytes}, true
		} else {
			return entry.Entry{Key: string(keyBytes), Value: valueBytes}, true
		}
	}

	return entry.Entry{}, false
}

// funkcija koja pronalazi ključ u index strukturi
func (rpo *ReadPath) FindInIndex(sstableName string, blockSize uint32, key string, stringLowerBound string, stringUpperBound string, summary *Summary, compression bool) (bool, uint32) {
	// sada prema lower bound, upper bound i summary-ju, odredimo donji i gornji offset za pretragu u indexu
	lowerOffset, upperOffset := rpo.SetOffsetsForIndexSearch(stringLowerBound, stringUpperBound, summary, compression)

	// sada proveravamo da li se ključ nalazi u opsegu lower i upper offseta u indexu
	// i pomoću block size znamo od kog bloka da krenemo sa pretragom
	// recimo ako je block size 50, a lower offset 66, onda znamo da ključ počinje od bloka 1 (blokovi se broje od 0)

	firstBlockIndex := lowerOffset / blockSize
	lastBlockIndex := upperOffset / blockSize

	// sada iteriramo kroz blokove i proveravamo da li se ključ nalazi u nekom od njih
	// učitamo sve blokove od firstBlockIndex do lastBlockIndex i zalepimo ih u jedan blok

	completeBlock := make([]byte, 0)

	for i := firstBlockIndex; i <= lastBlockIndex; i++ {
		block := rpo.BlockManager.BufferPool.GetBlock("sstables-"+sstableName+"-index", i)
		if block == nil {
			block = rpo.BlockManager.ReadBlock(SSTablesPath+sstableName+"/"+"index", i, blockSize)
			rpo.BlockManager.BufferPool.AddBlock(block)
		}

		// zalepimo blokove u jedan (radimo sa kopijama podataka)
		completeBlock = append(completeBlock, block.Data...) // proveriti da li je ovo uvek ispravno
	}

	/*
		VEOMA VAŽNO:
			ako zadnji bajt u bloku nije null bajt 0 ili nije newline blok, onda moramo pročitati još jedan blok posle njega
			jer se ostatak zapisa nalazi u tom bloku
			a i kasnije da nebi došlo do neke potencijalne greške u čitanju sadržaja blokova

			novi blok treba dodati i u slučaju koji je veoma redak a to je da zadnji bajt bude nula bajt
			a predzadnji bajt da bude bilo šta osim nula bajta ili newline bajta
			onda moramo pročitati još jedan blok
	*/

	// proveravamo da li je poslednji bajt u completeBlock-u null bajt ili newline
	if completeBlock[len(completeBlock)-1] != 0 && completeBlock[len(completeBlock)-1] != 10 {
		// pročitamo još jedan blok
		block := rpo.BlockManager.BufferPool.GetBlock("sstables-"+sstableName+"-index", lastBlockIndex+1)
		if block == nil {
			block = rpo.BlockManager.ReadBlock(SSTablesPath+sstableName+"/"+"index", lastBlockIndex+1, blockSize)
			rpo.BlockManager.BufferPool.AddBlock(block)

			// zalepimo blokove u jedan (radimo sa kopijama podataka)
			completeBlock = append(completeBlock, block.Data...) // proveriti da li je ovo uvek ispravno
		}
	} else if completeBlock[len(completeBlock)-1] == 0 && completeBlock[len(completeBlock)-2] != 0 {
		// pročitamo još jedan blok
		block := rpo.BlockManager.BufferPool.GetBlock("sstables-"+sstableName+"-index", lastBlockIndex+1)
		if block == nil {
			block = rpo.BlockManager.ReadBlock(SSTablesPath+sstableName+"/"+"index", lastBlockIndex+1, blockSize)
			rpo.BlockManager.BufferPool.AddBlock(block)

			// zalepimo blokove u jedan (radimo sa kopijama podataka)
			completeBlock = append(completeBlock, block.Data...) // proveriti da li je ovo uvek ispravno
		}
	}

	// sada iteriramo kroz completeBlock i proveravamo da li se ključ nalazi u njemu
	// odredjujemo koliko treba da umanjimo od lower i upper offseta da bi dobili tačan offset u completeBlock-u
	// i onda proveravamo da li se ključ nalazi u tom opsegu

	toSubtract := firstBlockIndex * blockSize

	clo := lowerOffset - toSubtract // corrected lower offset
	cuo := upperOffset - toSubtract // corrected upper offset

	// iteriramo kroz completeBlock i proveravamo da li se ključ nalazi u njemu
	for i := clo; i <= cuo; {
		// prvo pročitamo ključ
		potentialKeyBytes := ReadNullTerminatedString(completeBlock[i:])
		// nakon svakog čitanja se pomeramo za toliko
		i += uint32(len(potentialKeyBytes)) + 1 // +1 zbog null bajta

		// proveravamo da li je ključ jednak traženom ključu
		if compression {
			potentialKeyUint32, err := encoded_entry.VarintToUint32(potentialKeyBytes)
			HandleError(err, "Failed to convert varint to uint32")
			if rpo.BlockManager.BidirectionalMap.GetByUint32(potentialKeyUint32) == key {
				// pročitamo offset
				offsetBytes := encoded_entry.ReadVarint(completeBlock[i:])
				offset, err := encoded_entry.VarintToUint32(offsetBytes)
				HandleError(err, "Failed to convert varint to uint32")
				return true, offset
			}
		} else {
			if string(potentialKeyBytes) == key {
				// pročitamo offset
				offsetBytes := encoded_entry.ReadVarint(completeBlock[i:])
				offset, err := encoded_entry.VarintToUint32(offsetBytes)
				HandleError(err, "Failed to convert varint to uint32")
				return true, offset
			}
		}

		// pročitamo offset
		offsetBytes := encoded_entry.ReadVarint(completeBlock[i:])
		i += uint32(len(offsetBytes)) + 1 // +1 zbog newline bajta
	}

	return false, 0
}

// funkcija koja postavlja lower i upper offset za pretragu u indexu
func (rpo *ReadPath) SetOffsetsForIndexSearch(stringLowerBound string, stringUpperBound string, summary *Summary, compression bool) (uint32, uint32) {
	lowerOffset := uint32(0)
	upperOffset := uint32(0)

	if compression {
		for _, keyOffset := range summary.KeysOffsetsVarints { // tehnički uvek je varint, samo su ovde ključevi u varint formatu (za kompresiju)
			if rpo.BlockManager.BidirectionalMap.GetByUint32(keyOffset.Key) == stringLowerBound {
				lowerOffset = keyOffset.Offset
			}
			if rpo.BlockManager.BidirectionalMap.GetByUint32(keyOffset.Key) == stringUpperBound {
				upperOffset = keyOffset.Offset
			}
		}
	} else {
		for _, keyOffset := range summary.KeysOffsets {
			if string(keyOffset.Key) == stringLowerBound {
				lowerOffset = keyOffset.Offset
			}
			if string(keyOffset.Key) == stringUpperBound {
				upperOffset = keyOffset.Offset
			}
		}
	}

	/*
		korigujemo upper offset
		ako je 0, onda ga postavljamo na poslednji offset u summary-ju
		ako nije 0, onda ignorišemo samo...
	*/

	if upperOffset == 0 {
		if compression {
			upperOffset = summary.KeysOffsetsVarints[len(summary.KeysOffsetsVarints)-1].Offset
		} else {
			upperOffset = summary.KeysOffsets[len(summary.KeysOffsets)-1].Offset
		}
	}

	return lowerOffset, upperOffset
}

// funkcija koja postavlja lower i upper bound za pretragu
func (rpo *ReadPath) SetBounds(key string, summary *Summary, compression bool) (string, string) {

	stringLowerBound := ""
	stringUpperBound := ""
	if compression {
		stringLowerBound = rpo.BlockManager.BidirectionalMap.GetByUint32(summary.MinKeyVarint)
		stringUpperBound = rpo.BlockManager.BidirectionalMap.GetByUint32(summary.MaxKeyVarint)
	} else {
		stringLowerBound = string(summary.MinKey)
		stringUpperBound = string(summary.MaxKey)
	}

	return stringLowerBound, stringUpperBound
}

// funkcija koja koriguje upper bound za pretragu
func (rpo *ReadPath) CorrectUpperBound(key string, summary *Summary, compression bool) string {
	// jednostavno iteriramo kroz summarij i na koji naidjemo da je ključ < od njega, to je novi upper bound
	// idemo od kraja summarija
	stringUpperBound := ""
	if compression {
		for i := len(summary.KeysOffsetsVarints) - 1; i >= 0; i-- {
			if key < rpo.BlockManager.BidirectionalMap.GetByUint32(summary.KeysOffsetsVarints[i].Key) {
				stringUpperBound = rpo.BlockManager.BidirectionalMap.GetByUint32(summary.KeysOffsetsVarints[i].Key)
			}
		}
	} else {
		for i := len(summary.KeysOffsets) - 1; i >= 0; i-- {
			if key < string(summary.KeysOffsets[i].Key) {
				stringUpperBound = string(summary.KeysOffsets[i].Key)
			}
		}
	}

	return stringUpperBound
}

// funkcija koja koriguje lower bound za pretragu
func (rpo *ReadPath) CorrectLowerBound(key string, summary *Summary, compression bool) string {
	// jednostavno iteriramo kroz summarij i na koji naidjemo da je ključ >= od njega, to je novi lower bound
	stringLowerBound := ""
	if compression {
		for _, keyOffset := range summary.KeysOffsetsVarints {
			if key >= rpo.BlockManager.BidirectionalMap.GetByUint32(keyOffset.Key) {
				stringLowerBound = rpo.BlockManager.BidirectionalMap.GetByUint32(keyOffset.Key)
			}
		}
	} else {
		for _, keyOffset := range summary.KeysOffsets {
			if key >= string(keyOffset.Key) {
				stringLowerBound = string(keyOffset.Key)
			}
		}
	}

	return stringLowerBound
}

// funkcija koja printuje summary
func (summary *Summary) Print(compression bool) {

	// printujemo sve ključeve i offsete
	if compression {
		println("MinKey: ", summary.MinKeyVarint, " MaxKey: ", summary.MaxKeyVarint)
		for _, keyOffset := range summary.KeysOffsetsVarints {
			println("Key: ", keyOffset.Key, " Offset: ", keyOffset.Offset)
		}
	} else {
		println("MinKey: ", string(summary.MinKey), " MaxKey: ", string(summary.MaxKey))
		for _, keyOffset := range summary.KeysOffsets {
			println("Key: ", string(keyOffset.Key), " Offset: ", keyOffset.Offset)
		}
	}
}

// funkcija koja preko block managera pronalazi non-merge blokove summary-ja i deserijalizuje ih
func (rpo *ReadPath) FindAndDeserializeNONMergeSummary(sstableName string, blockSize uint32, compression bool) *Summary {
	// prvo kreiramo id prvog bloka
	blockID := "sstables-" + sstableName + "-summary"
	// pitamo block manager da li postoji blok sa zadatim id-em
	block := rpo.BlockManager.BufferPool.GetBlock(blockID, 0)
	if block == nil {
		// ako nema bloka, mora ga iskopati iz fajla
		block = rpo.BlockManager.ReadBlock(SSTablesPath+sstableName+"/"+"summary", 0, blockSize)
		rpo.BlockManager.BufferPool.AddBlock(block)
	}

	// prva 4 bajta su veličina summary-ja
	summarySizeBytes := block.Data[:4] // <-- na osnovu ovoga i block size procenimo koliko nam treba ješ blokova da učitamo
	// NAPOMENA: summary ima celobrojni broj blokova, zbog paddinga na zadnjem bloku
	summarySize := binary.BigEndian.Uint32(summarySizeBytes)

	summayBytes := make([]byte, 0) // ovde će biti svi summary bajtovi (PREMA SPECIFIKACIJI NALAZI SE U MEMORIJI ALI BLOKOVSKI UČITAVAMO)

	bytesToRead := summarySize // vremenom se umanjuje kako prolazimo kroz blokove
	// iteriramo kroz bajtove u nultom bloku
	for i := 4; i < len(block.Data); i++ {
		summayBytes = append(summayBytes, block.Data[i])
		bytesToRead--
		if bytesToRead == 0 {
			break
		}
	}

	// ako nismo pročitali sve, moramo pročitati i iz ostalih blokova
	for i := 1; bytesToRead > 0; i++ {
		block = rpo.BlockManager.BufferPool.GetBlock(blockID, uint32(i))
		if block == nil {
			block = rpo.BlockManager.ReadBlock(SSTablesPath+sstableName+"/"+"summary", uint32(i), blockSize)
			rpo.BlockManager.BufferPool.AddBlock(block)
		}

		// iteriramo kroz bajtove u bloku
		for j := 0; j < len(block.Data); j++ {
			summayBytes = append(summayBytes, block.Data[j])
			bytesToRead--
			if bytesToRead == 0 {
				break
			}
		}
	}

	// deserijalizujemo summary
	summary, err := DeserializeSummary(summayBytes, compression)
	HandleError(err, "Failed to deserialize summary")

	return summary
}

// funkcija koja deserijalizuje summary
func DeserializeSummary(summaryBytes []byte, compression bool) (*Summary, error) {
	summary := &Summary{}

	if compression { // odnosno ako smo pisali umesto ključa njegovu varint reprezentaciju
		// prvo pročitamo min i max ključ
		minKeyBytes := encoded_entry.ReadVarint(summaryBytes)
		minKeyVarint, err := encoded_entry.VarintToUint32(minKeyBytes)
		if err != nil {
			return nil, err
		}
		summary.MinKeyVarint = minKeyVarint

		// onda imamo 0 bajt
		summaryBytes = summaryBytes[len(minKeyBytes)+1:]

		maxKeyBytes := encoded_entry.ReadVarint(summaryBytes)
		maxKeyVarint, err := encoded_entry.VarintToUint32(maxKeyBytes)
		if err != nil {
			return nil, err
		}
		summary.MaxKeyVarint = maxKeyVarint

		// onda imamo 10 bajt (newline)
		summaryBytes = summaryBytes[len(maxKeyBytes)+1:]

		// iteriramo kroz summary bajtove (SVAKI RED JE TIPA ključ, 0, offset, 10)
		for len(summaryBytes) > 0 {
			key := encoded_entry.ReadVarint(summaryBytes)
			summaryBytes = summaryBytes[len(key)+1:]

			offset := encoded_entry.ReadVarint(summaryBytes)
			summaryBytes = summaryBytes[len(offset)+1:]

			offsetValue, err := encoded_entry.VarintToUint64(offset)
			if err != nil {
				return nil, err
			}

			keyValue, err := encoded_entry.VarintToUint64(key)
			if err != nil {
				return nil, err
			}
			summary.KeysOffsetsVarints = append(summary.KeysOffsetsVarints, KeyOffsetVarint{Key: uint32(keyValue), Offset: uint32(offsetValue)})
		}
	} else { // odnosno ako smo pisali ključ kao string (tj. niz bajtova samo)
		// prvo pročitamo min i max ključ
		// NAPOMENA: zapisi su u formatu ključ, 0, offset, 10 (bajtovi, 0 bajt)
		// prvo pročitamo min i max ključ (nemamo nigde za ključeve varint, čitamo do null bajta)
		minKey := ReadNullTerminatedString(summaryBytes)
		summary.MinKey = minKey
		// onda imamo 0 bajt
		summaryBytes = summaryBytes[len(minKey)+1:]

		maxKey := ReadNewlineTerminatedString(summaryBytes)
		summary.MaxKey = maxKey
		// onda imamo 10 bajt (newline)
		summaryBytes = summaryBytes[len(maxKey)+1:]

		// iteriramo kroz summary bajtove (SVAKI RED JE TIPA ključ, 0, offset, 10)
		for len(summaryBytes) > 0 {
			key := ReadNullTerminatedString(summaryBytes)
			summaryBytes = summaryBytes[len(key)+1:]

			// offset je u varint formatu
			offset := encoded_entry.ReadVarint(summaryBytes)
			summaryBytes = summaryBytes[len(offset)+1:]

			offsetValue, err := encoded_entry.VarintToUint64(offset)
			if err != nil {
				return nil, err
			}
			summary.KeysOffsets = append(summary.KeysOffsets, KeyOffset{Key: key, Offset: uint32(offsetValue)})
		}
	}

	return summary, nil
}

// funkcija koja čita null terminated string iz niza bajtova
func ReadNullTerminatedString(buf []byte) []byte {
	for i, b := range buf {
		if b == 0 {
			return buf[:i]
		}
	}
	return nil
}

// funkcija koja čita newline terminated string iz niza bajtova
func ReadNewlineTerminatedString(buf []byte) []byte {
	for i, b := range buf {
		if b == 10 {
			return buf[:i]
		}
	}
	return nil
}

// nisam hteo da menjam već postojeće funkcije, ali mi je trebala slična funkcionalnost
func ReadNullTerminatedStringBytes(buf []byte) ([]byte, bool) {
	for i, b := range buf {
		if b == 0 {
			return buf[:i], true
		}
	}
	return buf, false
}

func ReadNewlineTerminatedStringBytes(buf []byte) ([]byte, bool) {
	for i, b := range buf {
		if b == 10 {
			return buf[:i], true
		}
	}
	return buf, false
}

// funkcija koja preko block managera pronalazi non-merge blokove bloom filtera i deserijalizuje ih
func (rpo *ReadPath) FindAndDeserializeNONMergeBF(sstableName string, blockSize uint32) *probabilistics.BloomFilter {
	// prvo kreiramo id prvog bloka
	blockID := "sstables-" + sstableName + "-bloomfilter"
	// pitamo block manager da li postoji blok sa zadatim id-em
	block := rpo.BlockManager.BufferPool.GetBlock(blockID, 0)
	if block == nil {
		// ako nema bloka, mora ga iskopati iz fajla
		block = rpo.BlockManager.ReadBlock(SSTablesPath+sstableName+"/"+"bloomfilter", 0, blockSize)
		rpo.BlockManager.BufferPool.AddBlock(block)
	}

	// prva 4 bajta su veličina bloom filtera
	bloomFilterSizeBytes := block.Data[:4] // <-- na osnovu ovoga i block size procenimo koliko nam treba ješ blokova da učitamo
	bloomFilterSize := binary.BigEndian.Uint32(bloomFilterSizeBytes)

	bytesToRead := bloomFilterSize // vremenom se umanjuje kako prolazimo kroz blokove
	bloomFilterBytes := make([]byte, 0)

	// pročitamo koliko možemo iz nultog bloka, ako se bytes to read smanji na 0, znači da smo pročitali sve (napomena: zadnji blok ima padding u obliku binarnih nula)
	// iteriramo kroz bajtove u nultom bloku
	for i := 4; i < len(block.Data); i++ {
		bloomFilterBytes = append(bloomFilterBytes, block.Data[i])
		bytesToRead--
		if bytesToRead == 0 {
			break
		}
	}

	// ako nismo pročitali sve, moramo pročitati i iz ostalih blokova
	for i := 1; bytesToRead > 0; i++ {
		block = rpo.BlockManager.BufferPool.GetBlock(blockID, uint32(i))
		if block == nil {
			block = rpo.BlockManager.ReadBlock(SSTablesPath+sstableName+"/"+"bloomfilter", uint32(i), blockSize)
			rpo.BlockManager.BufferPool.AddBlock(block)
		}

		// iteriramo kroz bajtove u bloku
		for j := 0; j < len(block.Data); j++ {
			bloomFilterBytes = append(bloomFilterBytes, block.Data[j])
			bytesToRead--
			if bytesToRead == 0 {
				break
			}
		}
	}

	// deserijalizujemo bloom filter
	bloomFilter, err := probabilistics.DeserializeFromBytes_BF(bloomFilterBytes)
	HandleError(err, "Failed to deserialize bloom filter")

	return bloomFilter
}
