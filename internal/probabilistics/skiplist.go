package probabilistics

import (
	"fmt"
	"math/rand"
)

type Node struct {
	value  string
	right  *Node
	bottom *Node
}

type SkipList struct {
	head   *Node
	height int
}

// generišemo neku visinu
func roll(maxHeight int) int {
	level := 1
	for level < maxHeight && rand.Int31n(2) == 1 {
		level++
	}
	return level
}

// izgradimo 2 lanca levih i desnih čvorova
func NewSkipList(maxHeight int) *SkipList {
	highestLeft := &Node{value: "-∞"}
	highestRight := &Node{value: "+∞"}
	highestLeft.right = highestRight

	currentLeft := highestLeft
	currentRight := highestRight
	for i := 1; i < maxHeight; i++ {
		newLeft := &Node{value: "-∞"}
		newRight := &Node{value: "+∞"}

		currentLeft.bottom = newLeft
		currentRight.bottom = newRight

		newLeft.right = newRight

		currentLeft = newLeft
		currentRight = newRight
	}

	// vraćamo skip listu koja ima 2 reda (levih i desnih -+inf čvorova)
	return &SkipList{
		head:   highestLeft,
		height: maxHeight,
	}

}







func (skiplist *SkipList) Insert(value string) {

	if !skiplist.Search(value) {           // ispraviti kasnije neefikasno zbog duple pretrage
		current := skiplist.head
		// napravimo 2 reda izmedju kojih ubacujemo
		// nove čvorove, kako bi mogli lepo dapremostimo reference
		
		lefts := make([]*Node, skiplist.height)
		rights := make([]*Node, skiplist.height)

		for level := skiplist.height - 1; level >= 0; level-- {

			for current.right != nil && current.right.value != "+∞" && current.right.value < value {
				current = current.right
			}

			lefts[level] = current
			rights[level] = current.right
			
			if current.bottom != nil {
				current = current.bottom
			}
		}

		// koliko će visok biti novi niz čvorova?
		newlevel := roll(skiplist.height)

		// prikazujemo value i newlevel
		fmt.Printf("Inserting %s with level %d\n", value, newlevel)

		var previousNode *Node

		for i := 0; i < newlevel; i++ {

			newNode := &Node{
				value:  value,
				right:  rights[i],
				bottom: previousNode,
			}

			lefts[i].right = newNode
			previousNode = newNode
		}

		skiplist.PrintLevels()
		fmt.Println()
		fmt.Println()
	}
}











func (skiplist *SkipList) Search(value string) bool {
	current := skiplist.head

	for current != nil {
		for current.right != nil && current.right.value != "+∞" && current.right.value < value {
			current = current.right
		}

		if current.right != nil && current.right.value == value {
			return true
		}
		current = current.bottom
	}
	return false
}






func (skiplist *SkipList) PrintLevels() {
	// Počinjemo od najvišeg nivoa
	level := skiplist.height
	current := skiplist.head

	// Prolazimo kroz svaki nivo, počevši od najvišeg
	for level > 0 {
		fmt.Printf("Level %d: ", level)

		// Prolazimo kroz čvorove na trenutnom nivou
		node := current
		for node != nil {
			fmt.Printf("%s -> ", node.value)
			node = node.right
		}
		fmt.Println("nil") // Kraj linije za trenutni nivo

		// Idemo na sledeći nivo ispod
		if current.bottom != nil {
			current = current.bottom
		}
		level--
	}
}
