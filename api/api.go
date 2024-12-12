package api

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

const (
	reset  = "\033[0m"
	blue   = "\033[34m"
	green  = "\033[32m"
	yellow = "\033[33m"
	red    = "\033[31m"
	bold   = "\033[1m"
)

func StartCLI() {

	// ovo je i dalje probna faza, ne sme ovako ostati
	// ===============================================

	writePathObject := NewWritePath()
	writePathObject.BlockManager.FillBufferPool(writePathObject.WalManager.Wal.Path)
	for e := writePathObject.BlockManager.BufferPool.Pool.Front(); e != nil; e = e.Next() {
		// printamo niz bajtova
		fmt.Println(e.Value)
	}

	// ===============================================

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Println("\n" + bold + blue + "════════════════════════" + reset)
		fmt.Println(bold + green + "\nChoose an option:" + reset)
		fmt.Println("\n" + yellow + "1. PUT (key, value)" + reset)
		fmt.Println(yellow + "2. GET (key)" + reset)
		fmt.Println(yellow + "3. DELETE (key)" + reset)
		fmt.Println(red + "4. EXIT" + reset)
		fmt.Print("\n" + bold + blue + "════════════════════════\n\n" + reset)
		fmt.Print(bold + "➤ Enter your choice: " + reset)

		choice, _ := reader.ReadString('\n')
		switch choice {
		case "1\n":
			handlePut(writePathObject)
		case "2\n":
			handleGet()
		case "3\n":
			handleDelete()
		case "4\n":
			fmt.Println(bold + red + "\nExiting..." + reset)
			return
		default:
			fmt.Println(bold + red + "\nUnknown operation, please try again." + reset)
		}
	}
}

func handlePut(wpo *WritePath) {
	fmt.Print(bold + "\n➤ Enter key: " + reset)
	reader := bufio.NewReader(os.Stdin)
	key, _ := reader.ReadString('\n')

	key = strings.TrimSpace(key)

	if key == "" {
		fmt.Println(bold + red + "\nKey cannot be empty!" + reset)
		return
	}

	fmt.Print(bold + "\n➤ Enter value: " + reset)

	value, _ := reader.ReadString('\n')
	value = strings.TrimSpace(value)

	if value == "" {
		fmt.Println(bold + red + "\nValue cannot be empty!" + reset)
		return
	}

	wpo.WriteEntry(key, value)
	// printamo kako izgleda buffer pool sada
	for e := wpo.BlockManager.BufferPool.Pool.Front(); e != nil; e = e.Next() {
		fmt.Println(e.Value)
	}
	
}

func handleGet() {
	fmt.Println(bold + blue + "\nGET operation selected!" + reset)
}

func handleDelete() {
	fmt.Println(bold + yellow + "\nDELETE operation selected!" + reset)
}
