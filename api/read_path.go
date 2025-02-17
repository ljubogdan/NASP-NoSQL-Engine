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

				// nastaviće se... Bogdan

			}
		}
	}

	return entry.Entry{}, false
}

// funkcija koja pronalazi ključ u index strukturi
func (rpo *ReadPath) FindInIndex(sstableName string, blockSize uint32, key string, stringLowerBound string, stringUpperBound string, summary *Summary, compression bool) (bool, uint32) {
	// sada prema lower bound, upper bound i summary-ju, odredimo donji i gornji offset za pretragu u indexu
	lowerOffset, upperOffset := rpo.SetOffsetsForIndexSearch(stringLowerBound, stringUpperBound, summary, compression)

	// sada proveravamo da li se ključ nalazi u opsegu lower i upper offseta u indexu

	// Nastaviće se... Bogdan

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
		jako bitno naglasiti da upper offset mora biti veći
		od lower offseta, jer se u indexu ključevi nalaze u rastućem redosledu
		a ovde se može desiti (jako često) da upper offset ostane nula
		o čemu moramo voditi računa
		odnosno da će se koristiti upper offset samo u slučaju da je veći od lower offseta

		KADA JE UPPER OFFSET NULA, RADIMO PRETRAGU DO KRAJA FAJLA
	*/

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
