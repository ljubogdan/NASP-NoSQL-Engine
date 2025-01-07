package memtable

import (
	"fmt"
	"testing"
)

func TestMemtable(t *testing.T) {
	manager := NewMemtableManager()
	fmt.Println(manager.Insert("2", make([]byte, 2)))
	fmt.Println(manager.Insert("1", make([]byte, 1)))
	fmt.Println(manager.Insert("4", make([]byte, 4)))
	fmt.Println(manager.Insert("3", make([]byte, 3)))
	fmt.Println(manager.Insert("5", make([]byte, 5)))
	fmt.Println(manager.Insert("6", make([]byte, 6)))
	fmt.Println(manager.Insert("7", make([]byte, 7)))
	fmt.Println(manager.Insert("8", make([]byte, 8)))
	fmt.Println(manager.Insert("9", make([]byte, 9)))
	fmt.Println(manager.Insert("10", make([]byte, 10)))
	fmt.Println(manager.Insert("11", make([]byte, 0)))
	manager.Insert("7", make([]byte, 2))

	fmt.Println(manager.Find("2"))
	fmt.Println(manager.Find("11"))
	fmt.Println(manager.Find("asdafas"))

	manager.Delete("2")
	fmt.Println(manager.Find("2"))
	manager.Delete("11")
	fmt.Println(manager.Find("11"))
	manager.Delete("asdafas")

	for i := 0; i < len(manager.tables); i++ {
		fmt.Println(manager.tables[i].data.GetAll())
	}
}
