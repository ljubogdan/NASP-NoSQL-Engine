package probabilistics

import (
	"bytes"
	"crypto/md5" // nešto efikasnije?
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"os"
	"time"
)

type HashWithSeed struct {
	Seed []byte
}

func HandleError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %v", msg, err)
	}
}

// hash(vrednost + seed) ---> int od 8 bajta
func (hws HashWithSeed) Hash(data []byte) uint64 {
	hashObject := md5.New()
	hashObject.Write(append(data, hws.Seed...))
	return binary.BigEndian.Uint64(hashObject.Sum(nil))
}

func GenerateHashFunctions(number uint32) []HashWithSeed {
	hwsArray := make([]HashWithSeed, number)
	timestamp := uint32(time.Now().Unix())

	for i := uint32(0); i < number; i++ {
		seed := make([]byte, 4)
		binary.BigEndian.PutUint32(seed, timestamp+i)
		hwsArray[i] = HashWithSeed{Seed: seed}
	}

	return hwsArray
}

// M ---> opt. veličina niza, falsePositive ---> izmedju 0 i 1 (0 - 100%)
func CalculateM_BF(expectedElements uint32, falsePositiveRate float64) uint {
	return uint(math.Ceil(float64(expectedElements) * math.Abs(math.Log(falsePositiveRate)) / math.Pow(math.Log(2), float64(2))))
}

// m ---> veličina bitnog niza, K ---> optimalan broj hash funkcija
func CalculateK_BF(expectedElements uint32, m uint) uint {
	return uint(math.Ceil((float64(m) / float64(expectedElements)) * math.Log(2)))
}

type BloomFilter struct {
	bitArray      []bool // (zauzima 8 bita po elementu - go ne podržava bitove)
	hashFunctions []HashWithSeed
}

func NewBloomFilter(expectedElements uint32, falsePositiveRate float64) *BloomFilter {
	m := CalculateM_BF(expectedElements, falsePositiveRate)
	k := CalculateK_BF(expectedElements, m)
	bitArray := make([]bool, m)
	hashFunctions := GenerateHashFunctions(uint32(k))

	return &BloomFilter{bitArray: bitArray, hashFunctions: hashFunctions}
}

func (bf *BloomFilter) Add(data []byte) { // izmenjeno tako da radi sa nizm bajtova
	for _, hashFunction := range bf.hashFunctions {
		index := hashFunction.Hash(data) % uint64(len(bf.bitArray))
		bf.bitArray[index] = true
	}
}

func (bf *BloomFilter) Contains(data []byte) bool { // izmenjeno tako da radi sa nizom bajtova
	for _, hashFunction := range bf.hashFunctions {
		index := hashFunction.Hash(data) % uint64(len(bf.bitArray))
		if !bf.bitArray[index] {
			return false
		}
	}
	return true
}

// Serijalizacija i deserijalizacija

// [uint32 dužina niza][niz bajtova][uint32 broj hash funkcija][niz 4 bajta po hash funkciji]

func (bf *BloomFilter) Serialize(buffer *bytes.Buffer) error {

	// pre svega serijalizujemo celu dužinu bloom filtera a to je dužina bit niza + 4 bajta broja hash funkcija + 4 bajta po hash funkciji
	length := uint32(4 + len(bf.bitArray) + 4 + 4*len(bf.hashFunctions))
	if err := binary.Write(buffer, binary.BigEndian, length); err != nil {
		return fmt.Errorf("failed to write bloom filter length: %v", err)
	}

	if err := binary.Write(buffer, binary.BigEndian, uint32(len(bf.bitArray))); err != nil { // serijalizujemo dužinu niza
		return fmt.Errorf("failed to write bit array length: %v", err)
	}

	for _, bit := range bf.bitArray { // serijalizujemo niz
		var bitValue byte
		if bit {
			bitValue = 1
		} else {
			bitValue = 0
		}
		if err := buffer.WriteByte(bitValue); err != nil {
			return fmt.Errorf("failed to write bit: %v", err)
		}
	}

	if err := binary.Write(buffer, binary.BigEndian, uint32(len(bf.hashFunctions))); err != nil { // koliko imamo hash funkcija
		return fmt.Errorf("failed to write hash function count: %v", err)
	}

	for _, hashFunction := range bf.hashFunctions { // seed za svaku hash funkciju
		if _, err := buffer.Write(hashFunction.Seed); err != nil {
			return fmt.Errorf("failed to write seed: %v", err)
		}
	}

	return nil
}

func (bf *BloomFilter) SerializeToFile(filepath string) error {
	var buffer bytes.Buffer

	if err := bf.Serialize(&buffer); err != nil {
		return fmt.Errorf("failed to serialize bloom filter: %v", err)
	}

	file, err := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE, 0666)
	HandleError(err, "Failed to open file")
	defer file.Close()

	if _, err := file.Write(buffer.Bytes()); err != nil {
		return fmt.Errorf("failed to write to file: %v", err)
	}

	return nil
}

func DeserializeFromFile_BF(filename string) (*BloomFilter, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	bf := &BloomFilter{} // bolje od BloomFilter{} jer ne kopiramo ceo objekat

	// preskačemo prva 4 bajta jer predstavljaju dužinu koju smo upisali tokom SerializeToFile
	if _, err := file.Seek(4, 0); err != nil {
		return nil, fmt.Errorf("failed to seek file: %v", err)
	}

	var bitArrayLength uint32
	if err := binary.Read(file, binary.BigEndian, &bitArrayLength); err != nil {
		return nil, fmt.Errorf("failed to read bit array length: %v", err)
	}

	bf.bitArray = make([]bool, bitArrayLength)

	for i := uint32(0); i < bitArrayLength; i++ {
		bitValue := make([]byte, 1)
		if _, err := file.Read(bitValue); err != nil {
			return nil, fmt.Errorf("failed to read bit: %v", err)
		}

		if bitValue[0] == 1 {
			bf.bitArray[i] = true
		} else {
			bf.bitArray[i] = false
		}
	}

	var hashFunctionCount uint32
	if err := binary.Read(file, binary.BigEndian, &hashFunctionCount); err != nil {
		return nil, fmt.Errorf("failed to read hash function count: %v", err)
	}

	bf.hashFunctions = make([]HashWithSeed, hashFunctionCount)

	for i := uint32(0); i < hashFunctionCount; i++ {
		seed := make([]byte, 4)
		if _, err := file.Read(seed); err != nil {
			return nil, fmt.Errorf("failed to read seed: %v", err)
		}

		bf.hashFunctions[i] = HashWithSeed{Seed: seed}
	}

	return bf, nil
}

// funkcija koja će da deserijalizuje bloom filter iz niza bajtova
func DeserializeFromBytes_BF(data []byte) (*BloomFilter, error) {
	bf := &BloomFilter{}

	var bitArrayLength uint32

	reader := bytes.NewReader(data)

	// ne preskačemo prva 4 bajta jer ih nema
	if err := binary.Read(reader, binary.BigEndian, &bitArrayLength); err != nil {
		return nil, fmt.Errorf("failed to read bit array length: %v", err)
	}

	bf.bitArray = make([]bool, bitArrayLength)

	for i := uint32(0); i < bitArrayLength; i++ {
		bitValue := make([]byte, 1)
		if _, err := reader.Read(bitValue); err != nil {
			return nil, fmt.Errorf("failed to read bit: %v", err)
		}

		if bitValue[0] == 1 {
			bf.bitArray[i] = true
		} else {
			bf.bitArray[i] = false
		}
	}

	var hashFunctionCount uint32
	if err := binary.Read(reader, binary.BigEndian, &hashFunctionCount); err != nil {
		return nil, fmt.Errorf("failed to read hash function count: %v", err)
	}

	bf.hashFunctions = make([]HashWithSeed, hashFunctionCount)

	for i := uint32(0); i < hashFunctionCount; i++ {
		seed := make([]byte, 4)
		if _, err := reader.Read(seed); err != nil {
			return nil, fmt.Errorf("failed to read seed: %v", err)
		}

		bf.hashFunctions[i] = HashWithSeed{Seed: seed}
	}

	return bf, nil
}

// funkcija koja printuje izgled bloom filtera
func (bf *BloomFilter) Print() {
	fmt.Println("Bloom filter bit array:")
	for i, bit := range bf.bitArray {
		fmt.Printf("%d: %t\n", i, bit)
	}
	fmt.Println("Hash functions:")
	for i, hashFunction := range bf.hashFunctions {
		fmt.Printf("%d: %v\n", i, hashFunction)
	}
}
