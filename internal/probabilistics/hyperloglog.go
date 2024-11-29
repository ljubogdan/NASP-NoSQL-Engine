package probabilistics

import (
	"crypto/md5"
	"encoding/binary"
	"math"
	"math/bits"
)

const (
	HLL_MIN_PRECISION = 4
	HLL_MAX_PRECISION = 16
)

func firstKbits(value uint64, k uint8) uint64 {
	return value >> (64 - k)
}

func trailingZeroBits(value uint64) uint8 {
	return uint8(bits.TrailingZeros64(value))
}

type HyperLogLog struct {
	m   uint64
	p   uint8
	reg []uint8
}

func NewHyperLogLog(p uint8) *HyperLogLog {
	if p > HLL_MAX_PRECISION {
		p = HLL_MAX_PRECISION
	} else if p < HLL_MIN_PRECISION {
		p = HLL_MIN_PRECISION
	}

	m := uint64(math.Pow(2, float64(p)))
	return &HyperLogLog{p: p, m: m, reg: make([]uint8, m)}
}

func (hll *HyperLogLog) Add(value []byte) {
	hashObject := md5.New()
	hashObject.Write(value)
	hash64Bits := binary.BigEndian.Uint64(hashObject.Sum(nil))

	bucketIndex := firstKbits(hash64Bits, hll.p)
	zeroCount := trailingZeroBits(hash64Bits) + 1
	if hll.reg[bucketIndex] < zeroCount {
		hll.reg[bucketIndex] = zeroCount
	}
}

func (hll *HyperLogLog) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.reg {
		sum += math.Pow(math.Pow(2.0, float64(val)), -1)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.m))
	estimation := alpha * math.Pow(float64(hll.m), 2.0) / sum
	emptyRegs := hll.emptyCount()
	if estimation <= 2.5*float64(hll.m) { // do small range correction
		if emptyRegs > 0 {
			estimation = float64(hll.m) * math.Log(float64(hll.m)/float64(emptyRegs))
		}
	} else if estimation > 1/30.0*math.Pow(2.0, 32.0) { // do large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}

func (hll *HyperLogLog) emptyCount() int {
	sum := 0
	for _, val := range hll.reg {
		if val == 0 {
			sum++
		}
	}
	return sum
}

func (hll *HyperLogLog) Serialize() *[]byte {
	hllBytes := make([]byte, hll.m+1)
	hllBytes[0] = byte(hll.p)
	for i := uint64(0); i < hll.m; i++ {
		hllBytes[i+1] = byte(hll.reg[i])
	}
	return &hllBytes
}

func Deserialize_HLL(data *[]byte) *HyperLogLog {
	hll := NewHyperLogLog(uint8(0))
	regSize := uint64(len((*data)[1:]))
	for i := uint64(0); i < regSize; i++ {
		hll.reg[i] = uint8((*data)[i+1])
	}
	return hll
}
