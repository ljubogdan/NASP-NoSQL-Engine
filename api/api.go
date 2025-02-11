package api

import (
	"NASP-NoSQL-Engine/internal/block_manager"
	"NASP-NoSQL-Engine/internal/config"
	"NASP-NoSQL-Engine/internal/memtable"
	"NASP-NoSQL-Engine/internal/sstable"
	"NASP-NoSQL-Engine/internal/wal"
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"strings"
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
		fmt.Println(yellow + "3. DELETE (key)" + reset)
		fmt.Println(orange + "4. SETTINGS" + reset)
		fmt.Println(red + "5. EXIT" + reset)
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
			returnValue = handleGet(readPathObject)
		case "3\n":
			returnValue = handleDelete(deletePathObject)
		case "4\n":
			settings()
		case "5\n":
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

func handleGet(rpo *ReadPath) uint32 {
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
		return 0
	}
	return 5
}

func handleDelete(dpo *DeletePath) uint32 {
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

func settings() {
	fmt.Println(bold + orange + "\nSettings selected!" + reset)
}
