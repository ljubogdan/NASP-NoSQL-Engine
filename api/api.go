package api

import (
	"NASP-NoSQL-Engine/internal/block_manager"
	"NASP-NoSQL-Engine/internal/config"
	"NASP-NoSQL-Engine/internal/entry"
	"NASP-NoSQL-Engine/internal/memtable"
	"NASP-NoSQL-Engine/internal/probabilistics"
	"NASP-NoSQL-Engine/internal/sstable"
	"NASP-NoSQL-Engine/internal/tokenbucket"
	"NASP-NoSQL-Engine/internal/wal"
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

const (
	reset  = "\033[0m"
	blue   = "\033[34m"
	green  = "\033[32m"
	yellow = "\033[33m"
	red    = "\033[31m"
	bold   = "\033[1m"
	orange = "\033[38;5;208m"
)

func clearTerminal() {
	cmd := exec.Command("clear")
	cmd.Stdout = os.Stdout
	cmd.Run()
}

func message(returnValue uint32) {
	switch returnValue {
	case 0:
		fmt.Print(bold + green + "[OK]" + reset)
	case 1:
		fmt.Print(bold + red + "[ERROR] Key cannot be empty!" + reset)
	case 2:
		fmt.Print(bold + red + "[ERROR] Value cannot be empty!" + reset)
	case 3:
		fmt.Print(bold + red + "[ERROR] Entry size exceeds WAL size!" + reset)
	case 4:
		fmt.Print(bold + red + "[ERROR] Unknown operation, please try again!" + reset)
	case 5:
		fmt.Print(bold + orange + "[OK] Entry with given key doesnt exist!" + reset)
	case 6:
		fmt.Print(bold + red + "[ERROR] Rate limit exceeded!" + reset)
	case 7:
		fmt.Print(bold + red + "[ERROR] Unknown identifier!" + reset)
	default:
		fmt.Print(bold + red + "[ERROR] Unknown error." + reset)
	}
}

func StartCLI() {

	// ================================================================================================= LOWWATERMARK FUNKCIJA
	config.CorrectLowWatermark()
	// ================================================================================================= LOWWATERMARK FUNKCIJA

	blockManager := block_manager.NewBlockManager()

	blockManager.ReadFlushedCRCs()
	walManager := wal.NewWalManager()
	memtableManager := memtable.NewMemtableManager()
	sstableManager := sstable.NewSSTableManager()
	sstableManager.BlockManager = blockManager
	tokenBucket := tokenbucket.NewTokenBucket(5, time.Millisecond)

	writePathObject := NewWritePath(blockManager, walManager, memtableManager, sstableManager)
	writePathObject.BlockManager.FillWalPool(writePathObject.WalManager.Wal.Path)

	readPathObject := NewReadPath(blockManager, memtableManager, sstableManager)

	compaction := NewCompaction(readPathObject, writePathObject)

	entries := writePathObject.BlockManager.GetEntriesFromLeftoverWals()
	for _, entry := range entries {
		memtableManager.InsertFromWAL(&entry)
	}

	// ================================================================================================= SSTABELE LOAD
	sstableManager.LoadSSTables()
	// ================================================================================================= SSTABELE LOAD

	reader := bufio.NewReader(os.Stdin)
	returnValue := uint32(0)
	for {
		// =================================================================================================
		// sistema koji uklanja stare wal fajlove
		blockManager.DetectExpiredWals() // detektuje istekle wal fajlove i postavlja u configu low_watermark (poziva se uvek nakon put i delete operacije)
		walManager.LowWatermark = config.ReadLowWatermark()
		walManager.DeleteOldWals()
		// =================================================================================================

		iterators, level := compaction.CheckForCompaction()
		for len(*iterators) > 0 {
			compaction.Merge(*iterators, level)
			iterators, level = compaction.CheckForCompaction()
		}

		//clearTerminal()
		fmt.Println("\n" + bold + blue + "════════════════════════" + reset)
		fmt.Println(bold + green + "\nChoose an option:" + reset)
		fmt.Println("\n" + yellow + "1. PUT (key, value)" + reset)
		fmt.Println(yellow + "2. GET (key)" + reset)
		fmt.Println(yellow + "3. RANGE SCAN (min, max)" + reset)
		fmt.Println(yellow + "4. PREFIX SCAN (min, max)" + reset)
		fmt.Println(yellow + "5. DELETE (key)" + reset)
		fmt.Println(orange + "6. CHECK (sstable)" + reset)
		fmt.Println(orange + "7. SETTINGS" + reset)
		fmt.Println(red + "8. EXIT" + reset)
		fmt.Println(yellow + "9. HLL ADD" + reset)
		fmt.Println(yellow + "10. HLL COUNT" + reset)
		fmt.Println(yellow + "11. BF ADD" + reset)
		fmt.Println(yellow + "12. BF CHECK" + reset)
		fmt.Println(yellow + "13. CMS ADD" + reset)
		fmt.Println(yellow + "14. CMS COUNT" + reset)
		fmt.Print("\n" + bold + blue + "════════════════════════\n\n" + reset)

		fmt.Print("Status: ")
		message(returnValue)
		returnValue = 0

		fmt.Print(bold + "\n\n➤ Enter your choice: " + reset)

		choice, _ := reader.ReadString('\n')
		switch choice {
		case "1\n":
			returnValue = handlePut(writePathObject, compaction, tokenBucket)
		case "2\n":
			returnValue = handleGet(writePathObject, readPathObject, compaction, tokenBucket)
		case "3\n":
			returnValue = handleRangeScan(readPathObject, tokenBucket)
		case "4\n":
			returnValue = handlePrefixScan(readPathObject, tokenBucket)
		case "5\n":
			returnValue = handleDelete(writePathObject, compaction, tokenBucket)
		case "6\n":
			returnValue = handleCheck(readPathObject)
		case "7\n":
			settings()
		case "8\n":
			fmt.Println(bold + red + "\nExiting..." + reset)
			return
		case "9\n":
			returnValue = handleHLLAdd(writePathObject, readPathObject, compaction, tokenBucket)
		case "10\n":
			returnValue = handleHLLCount(readPathObject, tokenBucket)
		case "11\n":
			returnValue = handleBFAdd(writePathObject, readPathObject, compaction, tokenBucket)
		case "12\n":
			returnValue = handleBFCheck(readPathObject, tokenBucket)
		case "13\n":
			returnValue = handleCMSAdd(writePathObject, readPathObject, compaction, tokenBucket)
		case "14\n":
			returnValue = handleCMSCount(readPathObject, tokenBucket)
		default:
			returnValue = 4
		}
	}
}

func handlePut(wpo *WritePath, compaction *Compaction, tb *tokenbucket.TokenBucket) uint32 {
	if !tb.Allow(1) {
		return 6
	}

	fmt.Print(bold + "\n➤ Enter key: " + reset)
	reader := bufio.NewReader(os.Stdin)
	key, _ := reader.ReadString('\n')

	key = strings.TrimSpace(key)

	if key == "" {
		return 1
	}

	if strings.HasPrefix(key, "bf_") {
		fmt.Print(bold + "\n➤ Expected elements: " + reset)
		eStr, _ := reader.ReadString('\n')
		eStr = strings.TrimSpace(eStr)
		expected, err := strconv.Atoi(eStr)
		if err != nil || expected <= 0 {
			return 1
		}
		fmt.Print(bold + "\n➤ False positive rate: " + reset)
		fpStr, _ := reader.ReadString('\n')
		fpStr = strings.TrimSpace(fpStr)
		fp, err := strconv.ParseFloat(fpStr, 64)
		if err != nil {
			return 1
		}
		bf := probabilistics.NewBloomFilter(uint32(expected), fp)
		var buffer bytes.Buffer
		bf.Serialize(&buffer)
		return putValue(wpo, compaction, key, buffer.Bytes())
	}

	if strings.HasPrefix(key, "hll_") {
		fmt.Print(bold + "\n➤ Precision: " + reset)
		pStr, _ := reader.ReadString('\n')
		pStr = strings.TrimSpace(pStr)
		prec, err := strconv.Atoi(pStr)
		if err != nil {
			return 1
		}
		hll := probabilistics.NewHyperLogLog(uint8(prec))
		serialized := hll.Serialize()
		return putValue(wpo, compaction, key, *serialized)
	}

	if strings.HasPrefix(key, "cms_") {
		fmt.Print(bold + "\n➤ Epsilon: " + reset)
		epsStr, _ := reader.ReadString('\n')
		epsStr = strings.TrimSpace(epsStr)
		epsilon, err := strconv.ParseFloat(epsStr, 64)
		if err != nil {
			return 1
		}
		fmt.Print(bold + "\n➤ Delta: " + reset)
		dStr, _ := reader.ReadString('\n')
		dStr = strings.TrimSpace(dStr)
		delta, err := strconv.ParseFloat(dStr, 64)
		if err != nil {
			return 1
		}
		cms := probabilistics.NewCountMinSketch(epsilon, delta)
		var buffer bytes.Buffer
		cms.Serialize(&buffer)
		return putValue(wpo, compaction, key, buffer.Bytes())
	}

	fmt.Print(bold + "\n➤ Enter value: " + reset)

	value, _ := reader.ReadString('\n')
	value = strings.TrimSpace(value)

	if value == "" {
		return 2
	}

	return putValue(wpo, compaction, key, []byte(value))
}

func handleGet(wpo *WritePath, rpo *ReadPath, compaction *Compaction, tb *tokenbucket.TokenBucket) uint32 {
	if !tb.Allow(1) {
		return 6
	}

	fmt.Print(bold + "\n➤ Enter key: " + reset)
	reader := bufio.NewReader(os.Stdin)
	key, _ := reader.ReadString('\n')

	key = strings.TrimSpace(key)

	if key == "" {
		return 1
	}

	result, exists := rpo.ReadEntry(key)
	if !exists {
		return 5
	}

	rpo.BlockManager.CachePool.Add(&block_manager.CacheEntry{Key: key, Value: result.Value})

	if strings.HasPrefix(key, "bf_") {
		var bf *probabilistics.BloomFilter
		if len(result.Value) > 4 {
			var err error
			bf, err = probabilistics.DeserializeFromBytes_BF(result.Value[4:])
			if err != nil {
				bf = probabilistics.NewBloomFilter(config.ReadBloomFilterExpectedElements(), config.ReadBloomFilterFalsePositiveRate())
			}
		} else {
			bf = probabilistics.NewBloomFilter(config.ReadBloomFilterExpectedElements(), config.ReadBloomFilterFalsePositiveRate())
		}

		fmt.Println("\n1. ADD")
		fmt.Println("2. CHECK")
		fmt.Println("3. EXIT")
		fmt.Print(bold + "\n➤ Enter choice: " + reset)
		choice, _ := reader.ReadString('\n')
		switch choice {
		case "1\n":
			fmt.Print(bold + "\n➤ Enter value: " + reset)
			val, _ := reader.ReadString('\n')
			val = strings.TrimSpace(val)
			if val == "" {
				return 2
			}
			bf.Add([]byte(val))
			var buffer bytes.Buffer
			bf.Serialize(&buffer)
			return putValue(wpo, compaction, key, buffer.Bytes())
		case "2\n":
			fmt.Print(bold + "\n➤ Enter value: " + reset)
			val, _ := reader.ReadString('\n')
			val = strings.TrimSpace(val)
			if val == "" {
				return 2
			}
			if bf.Contains([]byte(val)) {
				fmt.Println(bold + "\n➤ Result: true" + reset)
			} else {
				fmt.Println(bold + "\n➤ Result: false" + reset)
			}
			return 0
		default:
			return 0
		}
	}

	if strings.HasPrefix(key, "hll_") {
		var hll *probabilistics.HyperLogLog
		if len(result.Value) > 0 {
			data := result.Value
			hll = probabilistics.Deserialize_HLL(&data)
		} else {
			hll = probabilistics.NewHyperLogLog(16)
		}

		fmt.Println("\n1. ADD")
		fmt.Println("2. COUNT")
		fmt.Println("3. EXIT")
		fmt.Print(bold + "\n➤ Enter choice: " + reset)
		choice, _ := reader.ReadString('\n')
		switch choice {
		case "1\n":
			fmt.Print(bold + "\n➤ Enter element: " + reset)
			val, _ := reader.ReadString('\n')
			val = strings.TrimSpace(val)
			if val == "" {
				return 2
			}
			hll.Add([]byte(val))
			serialized := hll.Serialize()
			return putValue(wpo, compaction, key, *serialized)
		case "2\n":
			fmt.Printf(bold+"\n➤ Count: %.0f"+reset+"\n", hll.Estimate())
			return 0
		default:
			return 0
		}
	}

	if strings.HasPrefix(key, "cms_") {
		var cms *probabilistics.CountMinSketch
		if len(result.Value) > 0 {
			var err error
			cms, err = probabilistics.DeserializeFromBytes_CMS(result.Value)
			if err != nil {
				cms = probabilistics.NewCountMinSketch(0.01, 0.01)
			}
		} else {
			cms = probabilistics.NewCountMinSketch(0.01, 0.01)
		}

		fmt.Println("\n1. ADD")
		fmt.Println("2. COUNT")
		fmt.Println("3. EXIT")
		fmt.Print(bold + "\n➤ Enter choice: " + reset)
		choice, _ := reader.ReadString('\n')
		switch choice {
		case "1\n":
			fmt.Print(bold + "\n➤ Enter value: " + reset)
			val, _ := reader.ReadString('\n')
			val = strings.TrimSpace(val)
			if val == "" {
				return 2
			}
			cms.Add(val)
			var buffer bytes.Buffer
			cms.Serialize(&buffer)
			return putValue(wpo, compaction, key, buffer.Bytes())
		case "2\n":
			fmt.Print(bold + "\n➤ Enter value: " + reset)
			val, _ := reader.ReadString('\n')
			val = strings.TrimSpace(val)
			if val == "" {
				return 2
			}
			count := cms.Count(val)
			fmt.Printf(bold+"\n➤ Count: %d"+reset+"\n", count)
			return 0
		default:
			return 0
		}
	}

	if len(result.Value) == 0 {
		return 5
	}

	fmt.Println(bold + "\n➤ Result: " + string(result.Value) + reset)
	return 0
}

func handleRangeScan(rpo *ReadPath, tb *tokenbucket.TokenBucket) uint32 {
	if !tb.Allow(1) {
		return 6
	}

	fmt.Print(bold + "\n➤ Enter minimum key (inclusive): " + reset)
	reader := bufio.NewReader(os.Stdin)
	min, _ := reader.ReadString('\n')

	min = strings.TrimSpace(min)
	if min == "" {
		return 1
	}

	fmt.Print(bold + "\n➤ Enter maximum key (inclusive): " + reset)
	max, _ := reader.ReadString('\n')

	max = strings.TrimSpace(max)
	if max == "" {
		return 1
	}

	if max < min {
		return 1
	}

	rangeScan := NewRangeScan(rpo, min, max)
	handlePageIteration(rpo, rangeScan, true)
	return 0
}

func handlePrefixScan(rpo *ReadPath, tb *tokenbucket.TokenBucket) uint32 {
	if !tb.Allow(1) {
		return 6
	}

	fmt.Print(bold + "\n➤ Enter the prefix: " + reset)
	reader := bufio.NewReader(os.Stdin)
	min, _ := reader.ReadString('\n')

	min = strings.TrimSpace(min)
	if min == "" {
		return 1
	}

	maxBytes := []byte(min)
	max := ""
	for i := len(maxBytes) - 1; i > -1; i-- {
		if maxBytes[i] < 255 {
			maxBytes[i] += 1
			max = string(maxBytes)
			break
		}
	}
	fmt.Println(max)

	rangeScan := NewRangeScan(rpo, min, max)
	handlePageIteration(rpo, rangeScan, false)
	return 0
}

func handleDelete(wpo *WritePath, compaction *Compaction, tb *tokenbucket.TokenBucket) uint32 {
	if !tb.Allow(1) {
		return 6
	}

	fmt.Print(bold + "\n➤ Enter key: " + reset)
	reader := bufio.NewReader(os.Stdin)
	key, _ := reader.ReadString('\n')

	key = strings.TrimSpace(key)

	if key == "" {
		return 1
	}

	returnValue := wpo.WriteEntryToWal(key, "")
	if returnValue == 0 {
		entries := wpo.MemtableManager.Delete(key)

		if len(*entries) > 0 {
			wpo.BlockManager.AddCRCsToCRCList(*entries)
			wpo.BlockManager.WriteFlushedCRCs()

			returnValue = wpo.WriteEntriesToSSTable(entries)
			iterators, level := compaction.CheckForCompaction()
			for len(*iterators) > 0 {
				compaction.Merge(*iterators, level)
				iterators, level = compaction.CheckForCompaction()
			}
		}
	}

	// ako je entry prisutan u kešu samo se apdejtuje
	wpo.BlockManager.CachePool.UpdateIfPresent(key, []byte("")) // alternativno dpo.BlockManager.CachePool.Delete(key) // potrebno apdejtovati dužine i ostale parametre ako ima potrebe

	return returnValue
}

func handlePageIteration(rpo *ReadPath, rangeScan *RangeScan, inclusive bool) {
	reader := bufio.NewReader(os.Stdin)
	choice := "1\n"
	pageNum := 1
	pageCache := make([][]entry.Entry, config.ReadPageCacheSize())
	cacheIndex := 0
	pageCache[cacheIndex] = *rangeScan.NextPage()
	for true {
		fmt.Println(bold + "\n➤ Page " + strconv.Itoa(pageNum) + ": " + reset)
		for i := 0; i < len(pageCache[cacheIndex]); i++ {
			if pageCache[cacheIndex][i].Key != rangeScan.max || inclusive {
				fmt.Print("\n   " + bold + strconv.Itoa(i+1) + ". " + pageCache[cacheIndex][i].Key + ": " + string(pageCache[cacheIndex][i].Value) + reset)
			}
		}
		fmt.Println()

		end := true
		for i := len(rangeScan.sstableEntries) - 1; i >= 0; i-- {
			if (rangeScan.sstableEntries[i].Key < rangeScan.max) || (inclusive && (rangeScan.sstableEntries[i].Key == rangeScan.max)) {
				end = false
				break
			}
		}
		end = end && (len(rangeScan.memtableEntries) < 1 || ((rangeScan.memtableEntries[0].Key == rangeScan.max) && !inclusive))
		end = end && cacheIndex == 0

		fmt.Println("\n" + bold + blue + "════════════════════════" + reset)
		fmt.Println(bold + green + "\nChoose an option:" + reset)
		if !end {
			fmt.Println("\n" + yellow + "1. NEXT" + reset)
		}
		if pageNum > 1 {
			fmt.Println(yellow + "2. BACK" + reset)
		}
		fmt.Println(red + "3. EXIT" + reset)
		fmt.Print("\n" + bold + blue + "════════════════════════\n" + reset)

		fmt.Print(bold + "\n➤ Enter your choice: " + reset)
		choice, _ = reader.ReadString('\n')
		if choice == "3\n" {
			break
		} else if choice == "2\n" && pageNum > 1 {
			pageNum--
			if len(pageCache)-1 > cacheIndex {
				cacheIndex++
			} else {
				rangeScan = NewRangeScan(rpo, rangeScan.min, rangeScan.max)
				for i := 0; i < pageNum; i++ {
					for j := len(pageCache) - 2; j > -1; j-- {
						pageCache[j+1] = pageCache[j]
					}
					pageCache[0] = *rangeScan.NextPage()
				}
				cacheIndex = 0
			}
		} else if choice == "1\n" && !end {
			if cacheIndex > 0 {
				cacheIndex--
			} else {
				for i := len(pageCache) - 2; i > -1; i-- {
					pageCache[i+1] = pageCache[i]
				}
				pageCache[0] = *rangeScan.NextPage()
			}
			pageNum++
		} else {
			message(4)
		}
	}
}

func handleCheck(rpo *ReadPath) uint32 {
	fmt.Print(bold + "\n➤ Enter sstable name (folder name): " + reset)
	reader := bufio.NewReader(os.Stdin)
	name, _ := reader.ReadString('\n')

	name = strings.TrimSpace(name)
	table := rpo.SSTablesManager.Get(name)
	if table == nil {
		return 7
	}

	blockInexes := rpo.CheckIntegrity(table)
	if len(blockInexes) == 0 {
		fmt.Println("There was no change detected in sstable data")
	} else {
		fmt.Println("There appears to be a change in block(s) with index(es): ", blockInexes)
	}
	return 0
}

func settings() {
	fmt.Println(bold + orange + "\nSettings selected!" + reset)
}

func putValue(wpo *WritePath, compaction *Compaction, key string, value []byte) uint32 {
	returnValue := wpo.WriteEntryToWal(key, string(value))
	if returnValue == 0 {
		entries := wpo.MemtableManager.Insert(key, value)

		if len(*entries) > 0 {
			wpo.BlockManager.AddCRCsToCRCList(*entries)
			wpo.BlockManager.WriteFlushedCRCs()

			returnValue = wpo.WriteEntriesToSSTable(entries)
			iterators, level := compaction.CheckForCompaction()
			for len(*iterators) > 0 {
				compaction.Merge(*iterators, level)
				iterators, level = compaction.CheckForCompaction()
			}
		}
	}

	wpo.BlockManager.CachePool.UpdateIfPresent(key, value)
	return returnValue
}

// ----------------------------- HyperLogLog -----------------------------
func handleHLLAdd(wpo *WritePath, rpo *ReadPath, compaction *Compaction, tb *tokenbucket.TokenBucket) uint32 {
	if !tb.Allow(1) {
		return 6
	}

	fmt.Print(bold + "\n➤ Enter key: " + reset)
	reader := bufio.NewReader(os.Stdin)
	key, _ := reader.ReadString('\n')
	key = strings.TrimSpace(key)
	if key == "" {
		return 1
	}

	fmt.Print(bold + "\n➤ Enter element: " + reset)
	val, _ := reader.ReadString('\n')
	val = strings.TrimSpace(val)
	if val == "" {
		return 2
	}

	result, exists := rpo.ReadEntry(key)
	var hll *probabilistics.HyperLogLog
	if exists && len(result.Value) > 0 {
		data := result.Value
		hll = probabilistics.Deserialize_HLL(&data)
	} else {
		hll = probabilistics.NewHyperLogLog(16)
	}

	hll.Add([]byte(val))
	serialized := hll.Serialize()
	return putValue(wpo, compaction, key, *serialized)
}

func handleHLLCount(rpo *ReadPath, tb *tokenbucket.TokenBucket) uint32 {
	if !tb.Allow(1) {
		return 6
	}

	fmt.Print(bold + "\n➤ Enter key: " + reset)
	reader := bufio.NewReader(os.Stdin)
	key, _ := reader.ReadString('\n')
	key = strings.TrimSpace(key)
	if key == "" {
		return 1
	}

	result, exists := rpo.ReadEntry(key)
	if !exists || len(result.Value) == 0 {
		fmt.Println(bold + "\n➤ Count: 0" + reset)
		return 0
	}
	data := result.Value
	hll := probabilistics.Deserialize_HLL(&data)
	fmt.Printf(bold+"\n➤ Count: %.0f"+reset+"\n", hll.Estimate())
	return 0
}

// ----------------------------- Bloom Filter -----------------------------
func handleBFAdd(wpo *WritePath, rpo *ReadPath, compaction *Compaction, tb *tokenbucket.TokenBucket) uint32 {
	if !tb.Allow(1) {
		return 6
	}

	fmt.Print(bold + "\n➤ Enter key: " + reset)
	reader := bufio.NewReader(os.Stdin)
	key, _ := reader.ReadString('\n')
	key = strings.TrimSpace(key)
	if key == "" {
		return 1
	}

	fmt.Print(bold + "\n➤ Enter value: " + reset)
	val, _ := reader.ReadString('\n')
	val = strings.TrimSpace(val)
	if val == "" {
		return 2
	}

	result, exists := rpo.ReadEntry(key)
	var bf *probabilistics.BloomFilter
	if exists && len(result.Value) > 4 {
		var err error
		bf, err = probabilistics.DeserializeFromBytes_BF(result.Value[4:])
		if err != nil {
			bf = probabilistics.NewBloomFilter(config.ReadBloomFilterExpectedElements(), config.ReadBloomFilterFalsePositiveRate())
		}
	} else {
		bf = probabilistics.NewBloomFilter(config.ReadBloomFilterExpectedElements(), config.ReadBloomFilterFalsePositiveRate())
	}

	bf.Add([]byte(val))
	var buffer bytes.Buffer
	bf.Serialize(&buffer)
	return putValue(wpo, compaction, key, buffer.Bytes())
}

func handleBFCheck(rpo *ReadPath, tb *tokenbucket.TokenBucket) uint32 {
	if !tb.Allow(1) {
		return 6
	}

	fmt.Print(bold + "\n➤ Enter key: " + reset)
	reader := bufio.NewReader(os.Stdin)
	key, _ := reader.ReadString('\n')
	key = strings.TrimSpace(key)
	if key == "" {
		return 1
	}

	fmt.Print(bold + "\n➤ Enter value: " + reset)
	val, _ := reader.ReadString('\n')
	val = strings.TrimSpace(val)
	if val == "" {
		return 2
	}

	result, exists := rpo.ReadEntry(key)
	if !exists || len(result.Value) <= 4 {
		fmt.Println(bold + "\n➤ Result: false" + reset)
		return 0
	}
	bf, err := probabilistics.DeserializeFromBytes_BF(result.Value[4:])
	if err != nil {
		fmt.Println(bold + "\n➤ Result: false" + reset)
		return 0
	}
	if bf.Contains([]byte(val)) {
		fmt.Println(bold + "\n➤ Result: true" + reset)
	} else {
		fmt.Println(bold + "\n➤ Result: false" + reset)
	}
	return 0
}

// ----------------------------- Count-Min Sketch -----------------------------
func handleCMSAdd(wpo *WritePath, rpo *ReadPath, compaction *Compaction, tb *tokenbucket.TokenBucket) uint32 {
	if !tb.Allow(1) {
		return 6
	}

	fmt.Print(bold + "\n➤ Enter key: " + reset)
	reader := bufio.NewReader(os.Stdin)
	key, _ := reader.ReadString('\n')
	key = strings.TrimSpace(key)
	if key == "" {
		return 1
	}

	fmt.Print(bold + "\n➤ Enter value: " + reset)
	val, _ := reader.ReadString('\n')
	val = strings.TrimSpace(val)
	if val == "" {
		return 2
	}

	result, exists := rpo.ReadEntry(key)
	var cms *probabilistics.CountMinSketch
	if exists && len(result.Value) > 0 {
		var err error
		cms, err = probabilistics.DeserializeFromBytes_CMS(result.Value)
		if err != nil {
			cms = probabilistics.NewCountMinSketch(0.01, 0.01)
		}
	} else {
		cms = probabilistics.NewCountMinSketch(0.01, 0.01)
	}

	cms.Add(val)
	var buffer bytes.Buffer
	cms.Serialize(&buffer)
	return putValue(wpo, compaction, key, buffer.Bytes())
}

func handleCMSCount(rpo *ReadPath, tb *tokenbucket.TokenBucket) uint32 {
	if !tb.Allow(1) {
		return 6
	}

	fmt.Print(bold + "\n➤ Enter key: " + reset)
	reader := bufio.NewReader(os.Stdin)
	key, _ := reader.ReadString('\n')
	key = strings.TrimSpace(key)
	if key == "" {
		return 1
	}

	fmt.Print(bold + "\n➤ Enter value: " + reset)
	val, _ := reader.ReadString('\n')
	val = strings.TrimSpace(val)
	if val == "" {
		return 2
	}

	result, exists := rpo.ReadEntry(key)
	if !exists || len(result.Value) == 0 {
		fmt.Println(bold + "\n➤ Count: 0" + reset)
		return 0
	}
	cms, err := probabilistics.DeserializeFromBytes_CMS(result.Value)
	if err != nil {
		fmt.Println(bold + "\n➤ Count: 0" + reset)
		return 0
	}
	count := cms.Count(val)
	fmt.Printf(bold+"\n➤ Count: %d"+reset+"\n", count)
	return 0
}
