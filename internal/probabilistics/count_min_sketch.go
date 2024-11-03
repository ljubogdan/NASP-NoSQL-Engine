package probabilistics

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"os"
)

// HashWithSeed imamo u bloom_filter.go
// Hash() takodje imamo u bloom_filter.go

// M --> širina matrice, epsilon --> greška
// (PRIM.) e = 0.1 (10%) --> kaže 100, realno izmedju 90-100
func CalculateM_CMS(epsilon float64) uint {
	return uint(math.Ceil(math.E / epsilon))
}

// K --> broj hash funkcija, delta --> greška
// (PRIM.) d = 0.1 (10%) --> šansa je 10% da epsilon predje 10% 
func CalculateK_CMS(delta float64) uint {
	return uint(math.Ceil(math.Log(math.E / delta)))
}


type CountMinSketch struct {
	matrix [][]uint32
	hashFunctions []HashWithSeed
	width uint32
	depth uint32
}

func NewCountMinSketch(epsilon float64, delta float64) *CountMinSketch {
	width := uint32(CalculateM_CMS(epsilon))
	depth := uint32(CalculateK_CMS(delta))

	matrix := make([][]uint32, depth) 
	for i := range matrix {
		matrix[i] = make([]uint32, width)
	}

	hashFunctions := GenerateHashFunctions(depth)

	return &CountMinSketch{
		matrix:       matrix,
        hashFunctions: hashFunctions,
        width:        width,
        depth:        depth,
	}
}

func (cms *CountMinSketch) Add(data string) {
	for i, hashFunction := range cms.hashFunctions {
		index := hashFunction.Hash([]byte(data)) % uint64(cms.width)
		cms.matrix[i][index] ++	
	}
}

func (cms *CountMinSketch) Count(data string) uint32 {
	minCount := uint32(math.MaxUint32)

	for i, hashFunction := range cms.hashFunctions {
		index := hashFunction.Hash([]byte(data)) % uint64(cms.width)
		count := cms.matrix[i][index]

		if count <= minCount {
			minCount = count
		}
	}

	return minCount
}

// serijalizacija i deserijalizacija
// [uint32 depth][uint32 width][uint32 matrix[depth][width]][uint32 broj hash funkcija][4 bajta po hash funkciji]

func (cms *CountMinSketch) Serialize(buffer *bytes.Buffer) error {
	if err := binary.Write(buffer, binary.BigEndian, uint32(cms.depth)); err != nil {
		return fmt.Errorf("failed to write depth: %v", err)
	}

	if err := binary.Write(buffer, binary.BigEndian, uint32(cms.width)); err != nil {
		return fmt.Errorf("failed to write width: %v", err)
	}

	// matrica
	for _, row := range cms.matrix {
		for _, cell := range row {
			if err := binary.Write(buffer, binary.BigEndian, cell); err != nil {
				return fmt.Errorf("failed to write matrix cell: %v", err)
			}
		}
	}

	if err := binary.Write(buffer, binary.BigEndian, uint32(len(cms.hashFunctions))); err != nil {
		return fmt.Errorf("failed to write hash function count: %v", err)
	}

	for _, hashFunction := range cms.hashFunctions {
		if _, err := buffer.Write(hashFunction.Seed); err != nil {
			return fmt.Errorf("failed to write seed: %v", err)
		}
	}

	return nil
}


func (cms *CountMinSketch) SerializeToFile(filename string) error {
	var buffer bytes.Buffer

	if err := cms.Serialize(&buffer); err != nil {
		return fmt.Errorf("failed to serialize CountMinSketch: %v", err)
	}

	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %v", err)
	}

	defer file.Close()

	if _, err := file.Write(buffer.Bytes()); err != nil {
		return fmt.Errorf("failed to write to file: %v", err)
	}

	return nil
}

func DeserializeFromFile_CMS(filename string) (*CountMinSketch, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %v", err)
	}

	defer file.Close()

	cms := &CountMinSketch{}

	var depth uint32
	if err := binary.Read(file, binary.BigEndian, &depth); err != nil {
		return nil, fmt.Errorf("failed to read depth: %v", err)
	}

	cms.depth = depth

	var width uint32
	if err := binary.Read(file, binary.BigEndian, &width); err != nil {
		return nil, fmt.Errorf("failed to read width: %v", err)
	}

	cms.width = width

	cms.matrix = make([][]uint32, cms.depth)
	for i := range cms.matrix {
		cms.matrix[i] = make([]uint32, cms.width)
	}

	for i := range cms.matrix {
		for j := range cms.matrix[i] {
			if err := binary.Read(file, binary.BigEndian, &cms.matrix[i][j]); err != nil {
				return nil, fmt.Errorf("failed to read matrix value: %v", err)
			}
		}
	}

	var hashFunctionCount uint32
	if err := binary.Read(file, binary.BigEndian, &hashFunctionCount); err != nil {
		return nil, fmt.Errorf("failed to read hash function count: %v", err)
	}

	cms.hashFunctions = make([]HashWithSeed, hashFunctionCount)
	for i := uint32(0); i < hashFunctionCount; i++ {
		seed := make([]byte, 4)
		if _, err := file.Read(seed); err != nil {
			return nil, fmt.Errorf("failed to read seed: %v", err)
		}
		cms.hashFunctions[i] = HashWithSeed{Seed: seed}
	}

	return cms, nil
}


