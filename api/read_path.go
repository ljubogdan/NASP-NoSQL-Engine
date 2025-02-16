package api

import (
	"NASP-NoSQL-Engine/internal/block_manager"
	"NASP-NoSQL-Engine/internal/encoded_entry"
	"NASP-NoSQL-Engine/internal/entry"
	"NASP-NoSQL-Engine/internal/memtable"
	"NASP-NoSQL-Engine/internal/probabilistics"
	"NASP-NoSQL-Engine/internal/sstable"
	"encoding/binary"
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
	MinKey           []byte // mora ovako da ostane jer ne znamo kako izgleda ključ, da li je bilo kompresije ili ne
	MaxKey           []byte
	KeysOffsets      []KeyOffset
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

func (rpo *ReadPath) ReadEntry(key string) (entry.Entry, bool) {
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
		// ako je merge:
		if sstable.Merge {
			continue
			// miljan
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

				// printujemo kako izgleda summary
				summary.Print(compression)

			}
		}
	}

	return entry.Entry{}, false
}

// funkcija koja printuje summary
func (summary *Summary) Print(compression bool) {
	// printujemo min i max ključ
	println("Min key: ", summary.MinKey)
	println("Max key: ", summary.MaxKey)

	// printujemo sve ključeve i offsete
	if compression {
		for _, keyOffset := range summary.KeysOffsetsVarints {
			println("Key: ", keyOffset.Key, " Offset: ", keyOffset.Offset)
		}
	} else {
		for _, keyOffset := range summary.KeysOffsets {
			println("Key: ", keyOffset.Key, " Offset: ", keyOffset.Offset)
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
		minKey := encoded_entry.ReadVarint(summaryBytes)
		summary.MinKey = minKey
		// onda imamo 0 bajt
		summaryBytes = summaryBytes[len(minKey)+1:]

		maxKey := encoded_entry.ReadVarint(summaryBytes)
		summary.MaxKey = maxKey
		// onda imamo 10 bajt (newline)
		summaryBytes = summaryBytes[len(maxKey)+1:]

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
