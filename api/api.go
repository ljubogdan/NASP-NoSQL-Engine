package api

import (
	"NASP-NoSQL-Engine/internal/block_manager"
	"NASP-NoSQL-Engine/internal/memtable"
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

	blockManager := block_manager.NewBlockManager()
	walManager := wal.NewWalManager()
	memtableManager := memtable.NewMemtableManager()

	writePathObject := NewWritePath(blockManager, walManager, memtableManager)
	writePathObject.BlockManager.FillBufferPool(writePathObject.WalManager.Wal.Path)

	readPathObject := NewReadPath(blockManager, memtableManager)

	deletePathObject := NewDeletePath(blockManager, walManager, memtableManager)

	//writePathObject.WalManager.SetLowWatermark(7)
	entries := writePathObject.BlockManager.GetEntriesFromLeftoverWals()
	for _, entry := range entries {
		memtableManager.InsertFromWAL(&entry)
	}

	reader := bufio.NewReader(os.Stdin)
	returnValue := uint32(0)
	for {
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

		// ako su entries prazni, nema ništa za upisati u sstable
		if len(*entries) > 0 {
			fmt.Println(entries)
			returnValue = wpo.WriteEntriesToSSTable(entries)
			return returnValue
		}
	}

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
	fmt.Println(bold + "\n➤ Result: " + string(result.Value) + reset)
	if exists {
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
		dpo.MemtableManager.Delete(key)    // ispraviti da se flushuje kada se napuni, mora vratiti entrije 
		return 0
	}

	return returnValue
}

func settings() {
	fmt.Println(bold + orange + "\nSettings selected!" + reset)
}
