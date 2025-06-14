package api

import (
	"NASP-NoSQL-Engine/internal/block_manager"
	"NASP-NoSQL-Engine/internal/config"
	"NASP-NoSQL-Engine/internal/entry"
	"NASP-NoSQL-Engine/internal/memtable"
	"NASP-NoSQL-Engine/internal/sstable"
	"NASP-NoSQL-Engine/internal/tokenbucket"
	"NASP-NoSQL-Engine/internal/wal"
	"bufio"
	"fmt"
	"os"
	"os/exec"
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

	deletePathObject := NewDeletePath(blockManager, walManager, memtableManager, sstableManager)

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

		//clearTerminal()
		fmt.Println("\n" + bold + blue + "════════════════════════" + reset)
		fmt.Println(bold + green + "\nChoose an option:" + reset)
		fmt.Println("\n" + yellow + "1. PUT (key, value)" + reset)
		fmt.Println(yellow + "2. GET (key)" + reset)
		fmt.Println(yellow + "3. RANGE SCAN (min, max)" + reset)
		fmt.Println(yellow + "4. PREFIX SCAN (min, max)" + reset)
		fmt.Println(yellow + "5. DELETE (key)" + reset)
		fmt.Println(orange + "6. SETTINGS" + reset)
		fmt.Println(red + "7. EXIT" + reset)
		fmt.Print("\n" + bold + blue + "════════════════════════\n\n" + reset)

		fmt.Print("Status: ")
		message(returnValue)
		returnValue = 0

		fmt.Print(bold + "\n\n➤ Enter your choice: " + reset)

		choice, _ := reader.ReadString('\n')
		switch choice {
		case "1\n":
			returnValue = handlePut(writePathObject)
		case "2\n":
			returnValue = handleGet(readPathObject, tokenBucket)
		case "3\n":
			returnValue = handleRangeScan(readPathObject, tokenBucket)
		case "4\n":
			returnValue = handlePrefixScan(readPathObject, tokenBucket)
		case "5\n":
			returnValue = handleDelete(deletePathObject, tokenBucket)
		case "6\n":
			settings()
		case "7\n":
			fmt.Println(bold + red + "\nExiting..." + reset)
			return
		default:
			returnValue = 4
		}
	}
}

func handlePut(wpo *WritePath) uint32 {
	fmt.Print(bold + "\n➤ Enter key: " + reset)
	reader := bufio.NewReader(os.Stdin)
	key, _ := reader.ReadString('\n')

	key = strings.TrimSpace(key)

	if key == "" {
		return 1
	}

	fmt.Print(bold + "\n➤ Enter value: " + reset)

	value, _ := reader.ReadString('\n')
	value = strings.TrimSpace(value)

	if value == "" {
		return 2
	}

	returnValue := wpo.WriteEntryToWal(key, value) // upisuje u wal
	if returnValue == 0 {
		entries := wpo.MemtableManager.Insert(key, []byte(value)) // upisuje u memtable

		// ako je dužina entrija veća od nula:
		if len(*entries) > 0 {
			// upisujemo CRC-ove u block manager listu
			wpo.BlockManager.AddCRCsToCRCList(*entries)
			wpo.BlockManager.WriteFlushedCRCs()

			returnValue = wpo.WriteEntriesToSSTable(entries)
		}
	}

	// ako je entry prisutan u kešu samo se apdejtuje
	wpo.BlockManager.CachePool.UpdateIfPresent(key, []byte(value))

	return returnValue
}

func handleGet(rpo *ReadPath, tb *tokenbucket.TokenBucket) uint32 {
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
	if exists {
		fmt.Println(bold + "\n➤ Result: " + string(result.Value) + reset)

		// ubacujemo entry u key cache
		rpo.BlockManager.CachePool.Add(&block_manager.CacheEntry{Key: key, Value: result.Value})

		return 0
	}
	return 5
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

func handleDelete(dpo *DeletePath, tb *tokenbucket.TokenBucket) uint32 {
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

	returnValue := dpo.WriteEntryToWal(key, "")
	if returnValue == 0 {
		entries := dpo.MemtableManager.Delete(key)

		if len(*entries) > 0 {
			dpo.BlockManager.AddCRCsToCRCList(*entries)
			dpo.BlockManager.WriteFlushedCRCs()

			returnValue = dpo.WriteEntriesToSSTable(entries)
		}
	}

	// ako je entry prisutan u kešu samo se apdejtuje
	dpo.BlockManager.CachePool.UpdateIfPresent(key, []byte("")) // alternativno dpo.BlockManager.CachePool.Delete(key) // potrebno apdejtovati dužine i ostale parametre ako ima potrebe

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
		fmt.Println(bold + "\n➤ Page " + string(pageNum+48) + ": " + reset)
		for i := 0; i < len(pageCache[cacheIndex]); i++ {
			if pageCache[cacheIndex][i].Key != rangeScan.max || inclusive {
				fmt.Print("\n   " + bold + string(i+49) + ". " + pageCache[cacheIndex][i].Key + ": " + string(pageCache[cacheIndex][i].Value) + reset)
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

func settings() {
	fmt.Println(bold + orange + "\nSettings selected!" + reset)
}
