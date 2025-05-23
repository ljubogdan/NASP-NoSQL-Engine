package probabilistics

import (
	"os"
	"testing"
	"strconv"
	"fmt"
	"NASP-NoSQL-Engine/internal/entry"
	"reflect"
)

// go test -v ./internal/probabilistics -run TestBloomFilter

func TestBloomFilter(t *testing.T) {
	bf := NewBloomFilter(100, 0.01)
	bf.Add([]byte("hello"))
	bf.Add([]byte("world"))
	bf.Add([]byte("!"))

	if !bf.Contains([]byte("hello")) {
		t.Errorf("Bloom filter doesn't contain 'hello'")
	}

	if !bf.Contains([]byte("world")) {
		t.Errorf("Bloom filter doesn't contain 'world'")
	}

	if !bf.Contains([]byte("!")) {
		t.Errorf("Bloom filter doesn't contain '!'")
	}

	if bf.Contains([]byte("foo")) {
		t.Errorf("Bloom filter contains 'foo'")
	}

	if bf.Contains([]byte("bar")) {
		t.Errorf("Bloom filter contains 'bar'")
	}

	filename := "bloomfilter_test.dat"
	if err := bf.SerializeToFile(filename); err != nil {
		t.Fatalf("failed to serialize Bloom filter: %v", err)
	}
	defer os.Remove(filename)

	deserializedBF, err := DeserializeFromFile_BF(filename)
	if err != nil {
		t.Fatalf("failed to deserialize Bloom filter: %v", err)
	}

	if !deserializedBF.Contains([]byte("hello")) {
		t.Errorf("Deserialized Bloom filter doesn't contain 'hello'")
	}

	if !deserializedBF.Contains([]byte("world")) {
		t.Errorf("Deserialized Bloom filter doesn't contain 'world'")
	}

	if !deserializedBF.Contains([]byte("!")) {
		t.Errorf("Deserialized Bloom filter doesn't contain '!'")
	}

	if deserializedBF.Contains([]byte("foo")) {
		t.Errorf("Deserialized Bloom filter contains 'foo'")
	}

	if deserializedBF.Contains([]byte("bar")) {
		t.Errorf("Deserialized Bloom filter contains 'bar'")
	}
}

// go test -v ./internal/probabilistics -run TestCountMinSketch

func TestCountMinSketch(t *testing.T) {
	cms := NewCountMinSketch(0.01, 0.01)

	cms.Add("apple")
	cms.Add("banana")
	cms.Add("apple")
	cms.Add("orange")
	cms.Add("banana")
	cms.Add("apple")

	if count := cms.Count("apple"); count < 3 {
		t.Errorf("Expected 'apple' count to be at least 3, got %d", count)
	}

	if count := cms.Count("banana"); count < 2 {
		t.Errorf("Expected 'banana' count to be at least 2, got %d", count)
	}

	if count := cms.Count("orange"); count < 1 {
		t.Errorf("Expected 'orange' count to be at least 1, got %d", count)
	}

	if count := cms.Count("grape"); count != 0 {
		t.Errorf("Expected 'grape' count to be 0 or close to 0, got %d", count)
	}

	filename := "countminsketch_test.dat"
	if err := cms.SerializeToFile(filename); err != nil {
		t.Fatalf("Failed to serialize Count-Min Sketch: %v", err)
	}
	defer os.Remove(filename)

	deserializedCMS, err := DeserializeFromFile_CMS(filename)
	if err != nil {
		t.Fatalf("Failed to deserialize Count-Min Sketch: %v", err)
	}

	if count := deserializedCMS.Count("apple"); count < 3 {
		t.Errorf("Deserialized Count-Min Sketch: Expected 'apple' count to be at least 3, got %d", count)
	}

	if count := deserializedCMS.Count("banana"); count < 2 {
		t.Errorf("Deserialized Count-Min Sketch: Expected 'banana' count to be at least 2, got %d", count)
	}

	if count := deserializedCMS.Count("orange"); count < 1 {
		t.Errorf("Deserialized Count-Min Sketch: Expected 'orange' count to be at least 1, got %d", count)
	}

	if count := deserializedCMS.Count("grape"); count != 0 {
		t.Errorf("Deserialized Count-Min Sketch: Expected 'grape' count to be 0 or close to 0, got %d", count)
	}
}

// go test -v ./internal/probabilistics -run TestSkipList

func TestSkipList(t *testing.T) {
	skiplist := NewSkipList(5) // Kreiramo skip listu sa maksimalnom visinom 5

	// Umetanje elemenata
	elements := []string{"apple", "banana", "cherry", "date", "elderberry", "fig", "grape", "honeydew", "kiwi", "lemon"}
	for _, elem := range elements {
		skiplist.Insert(entry.Entry{Key: elem, Value: nil})
	}

	// Testiranje pretrage umetnutih elemenata
	for _, elem := range elements {
		if !skiplist.Search(entry.Entry{Key: elem, Value: nil}) {
			t.Errorf("SkipList should contain '%s'", elem)
		}
	}

	// Testiranje pretrage za elemente koji nisu umetnuti
	nonElements := []string{"mango", "nectarine", "orange", "papaya", "quince"}
	for _, elem := range nonElements {
		if skiplist.Search(entry.Entry{Key: elem, Value: nil}) {
			t.Errorf("SkipList should not contain '%s'", elem)
		}
	}

	// Testiranje umetanja i pretrage dodatnih elemenata
	additionalElements := []string{"raspberry", "strawberry", "tangerine", "ugli", "vanilla"}
	for _, elem := range additionalElements {
		skiplist.Insert(entry.Entry{Key: elem, Value: nil})
		if !skiplist.Search(entry.Entry{Key: elem, Value: nil}) {
			t.Errorf("SkipList should contain '%s'", elem)
		}
	}

	// Testiranje umetanja duplikata
	for _, elem := range elements {
		skiplist.Insert(entry.Entry{Key: elem, Value: nil})
		if !skiplist.Search(entry.Entry{Key: elem, Value: nil}) {
			t.Errorf("SkipList should contain '%s' even after inserting duplicate", elem)
		}
	}

	skiplist.PrintLevels()

	// ispisuje type koje je vrste jedan node value
	fmt.Println(reflect.TypeOf(skiplist.head.value))

	// testiranja Get po string ključu
	for _, elem := range elements {
		entry, found := skiplist.Get(elem)
		fmt.Println(entry, found)
	}

	entry, found := skiplist.Get("bonanzaaa")
	fmt.Println(entry.Key, found)

	// testiranje GetAll

	entries := skiplist.GetAll()

	for _, entry := range entries {
		fmt.Println(entry)
	}


	// testiranje Size
	fmt.Println(skiplist.Size())

}

func TestHyperLogLog(t *testing.T) {
	hll := NewHyperLogLog(16)
	uniqueElementCount := uint32(100000)

	for i := uint32(1); i < uniqueElementCount; i++ {
		numberString := strconv.Itoa(int(i))    // proveriti
		hll.Add([]byte(numberString))
	}

	count := hll.Estimate()
	difference := count - float64(uniqueElementCount)
	if difference > float64(uniqueElementCount)*0.01 || difference < float64(uniqueElementCount)*(-0.01) {
		t.Errorf("Estimation HyperLogLog: Expected unique element count to be witing 1%%  of %d, got %f", uniqueElementCount, count)
	}

	deserializedHLL := Deserialize_HLL(hll.Serialize())
	if deserializedCount := deserializedHLL.Estimate(); count != deserializedCount {
		t.Errorf("Deserialized HyperLogLog: Expected estimation after deserialization to be the same, got %f", deserializedCount)
	}
}
