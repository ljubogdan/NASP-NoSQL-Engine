package trees

import (
	"NASP-NoSQL-Engine/internal/entry"
	"fmt"
)

type BTreeNode struct {
	isLeaf   bool
	keys     []string
	values   []entry.Entry
	children []*BTreeNode
	t        int
}

func NewBTreeNode(t int, isLeaf bool) *BTreeNode {
	return &BTreeNode{
		isLeaf:   isLeaf,
		keys:     make([]string, 0),
		values:   make([]entry.Entry, 0),
		children: make([]*BTreeNode, 0),
		t:        t,
	}
}

type BTree struct {
	root *BTreeNode
	t    int
}

func NewBTree(t int) *BTree {
	return &BTree{root: nil, t: t}
}

func (node *BTreeNode) splitChild(i int, t int) {
	if node.isLeaf {
		// fmt.Println("pokušaj deljenja list čvora!")
		return
	}

	if i < 0 || i >= len(node.children) {
		// fmt.Printf("indeks %d je van opsega, children count: %d\n", i, len(node.children))
		return
	}

	if len(node.children) == 0 {
		// fmt.Println("pokušaj deljenja čvora koji nema decu!")
		return
	}

	child := node.children[i]

	if len(child.keys) < 2*t-1 {
		// fmt.Printf("nedovoljno ključeva za sečenje, očekivano: %d, trenutno: %d\n", 2*t-1, len(child.keys))
		return
	}

	newChild := NewBTreeNode(t, child.isLeaf)
	middleKey := child.keys[t-1]
	middleValue := child.values[t-1]

	newChild.keys = append(newChild.keys, child.keys[t:]...)
	newChild.values = append(newChild.values, child.values[t:]...)
	child.keys = child.keys[:t-1]
	child.values = child.values[:t-1]

	if !child.isLeaf {
		newChild.children = append(newChild.children, child.children[t:]...)
		child.children = child.children[:t]
	}

	node.children = append(node.children[:i+1], append([]*BTreeNode{newChild}, node.children[i+1:]...)...)
	node.keys = append(node.keys[:i], append([]string{middleKey}, node.keys[i:]...)...)
	node.values = append(node.values[:i], append([]entry.Entry{middleValue}, node.values[i:]...)...)
}

func (node *BTreeNode) insertNonFull(k string, v entry.Entry) {
	i := len(node.keys) - 1

	for j, key := range node.keys {
		if key == k {
			node.values[j] = v
			return
		}
	}

	if node.isLeaf {
		node.keys = append(node.keys, "")
		node.values = append(node.values, entry.Entry{})
		for i >= 0 && node.keys[i] > k {
			node.keys[i+1] = node.keys[i]
			node.values[i+1] = node.values[i]
			i--
		}
		node.keys[i+1] = k
		node.values[i+1] = v
	} else {
		for i >= 0 && node.keys[i] > k {
			i--
		}
		i++
		if len(node.children[i].keys) == 2*node.t-1 {
			node.splitChild(i, node.t)
			if node.keys[i] < k {
				i++
			}
		}
		node.children[i].insertNonFull(k, v)
	}
}

func (tree *BTree) Insert(data entry.Entry) {
	k := data.Key

	if tree.root == nil {
		// fmt.Println("inicijalizacija korena stabla...")
		tree.root = NewBTreeNode(tree.t, true)
		tree.root.keys = append(tree.root.keys, k)
		tree.root.values = append(tree.root.values, data)
		return
	}

	if len(tree.root.keys) == 2*tree.t-1 {
		// fmt.Println("korenski čvor pun, delimo ga...")

		newRoot := NewBTreeNode(tree.t, false)

		newRoot.children = append(newRoot.children, tree.root)

		newRoot.splitChild(0, tree.t)

		i := 0
		if newRoot.keys[0] < k {
			i++
		}

		newRoot.children[i].insertNonFull(k, data)

		tree.root = newRoot
	} else {
		tree.root.insertNonFull(k, data)
	}
}

func (tree *BTree) Delete(key string) {
	if tree.root == nil {
		fmt.Println("Stablo je prazno.")
		return
	}

	data, found := tree.Get(key)
	if found {
		data.Tombstone = 1
		tree.Insert(data)
	} else {
		fmt.Println("Ključ nije pronađen.")
	}
}

func (node *BTreeNode) Get(key string) (entry.Entry, bool) {
	i := 0
	for i < len(node.keys) && key > node.keys[i] {
		i++
	}
	if i < len(node.keys) && node.keys[i] == key {
		return node.values[i], true
	}
	if node.isLeaf {
		return entry.Entry{}, false
	}
	return node.children[i].Get(key)
}

func (tree *BTree) Get(key string) (entry.Entry, bool) {
	if tree.root == nil {
		return entry.Entry{}, false
	}
	return tree.root.Get(key)
}

func (node *BTreeNode) GetAll() []entry.Entry {
	var result []entry.Entry
	for i := 0; i < len(node.keys); i++ {
		if !node.isLeaf {
			result = append(result, node.children[i].GetAll()...)
		}
		result = append(result, node.values[i])
	}
	if !node.isLeaf {
		result = append(result, node.children[len(node.keys)].GetAll()...)
	}
	return result
}

func (tree *BTree) GetAll() []entry.Entry {
	if tree.root == nil {
		return []entry.Entry{}
	}
	return tree.root.GetAll()
}

func (node *BTreeNode) Size() int {
	size := len(node.keys)
	if !node.isLeaf {
		for _, child := range node.children {
			size += child.Size()
		}
	}
	return size
}

func (tree *BTree) Size() int {
	if tree.root == nil {
		return 0
	}
	return tree.root.Size()
}

func (node *BTreeNode) Print(level int) {
	fmt.Printf("Level %d\n", level)
	for i, key := range node.keys {
		fmt.Printf("  Key: %s, Value: %v\n", key, node.values[i])
	}
	for _, child := range node.children {
		child.Print(level + 1)
	}
}

func (tree *BTree) Print() {
	if tree.root != nil {
		tree.root.Print(0)
	}
}

/*
// testiranje
func main() {
	btree := NewBTree(3)
	btree.Insert(entry.Entry{
		Key:   "jabuka",
		Value: []byte{1, 2, 3},
	})
	btree.Insert(entry.Entry{
		Key:   "banana",
		Value: []byte{4, 5, 6},
	})
	btree.Insert(entry.Entry{
		Key:   "visnja",
		Value: []byte{7, 8, 9},
	})
	btree.Insert(entry.Entry{
		Key:   "boronica",
		Value: []byte{10, 11, 12},
	})
	btree.Insert(entry.Entry{
		Key:   "steak",
		Value: []byte{13, 14, 15},
	})
	btree.Insert(entry.Entry{
		Key:   "jaja",
		Value: []byte{16, 17, 18},
	})
	btree.Insert(entry.Entry{
		Key:   "burger",
		Value: []byte{19, 20, 21},
	})

	btree.Print()


	entry, found := btree.Get("banana")
	if found {
		fmt.Printf("Pronađen ključ: %s, vrednost: %v\n", entry.Key, entry.Value)
	} else {
		fmt.Println("Ključ nije pronađen.")
	}
}
*/
