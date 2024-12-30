package trees

import (
	"fmt"
)

type BTreeNode struct {
	isLeaf   bool
	keys     []string
	values   [][]byte
	children []*BTreeNode
	t        int
}

func NewBTreeNode(t int, isLeaf bool) *BTreeNode {
	return &BTreeNode{
		isLeaf:   isLeaf,
		keys:     make([]string, 0),
		values:   make([][]byte, 0),
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

// Splitovanje prepunog čvora
func (node *BTreeNode) splitChild(i int, t int) {
	child := node.children[i]
	newChild := NewBTreeNode(t, child.isLeaf)

	newChild.keys = append(newChild.keys, child.keys[t:]...)
	newChild.values = append(newChild.values, child.values[t:]...)
	child.keys = child.keys[:t-1]
	child.values = child.values[:t-1]

	if !child.isLeaf {
		newChild.children = append(newChild.children, child.children[t:]...)
		child.children = child.children[:t]
	}

	node.children = append(node.children[:i+1], append([]*BTreeNode{newChild}, node.children[i+1:]...)...)
	node.keys = append(node.keys[:i], append([]string{child.keys[t-1]}, node.keys[i:]...)...)
	node.values = append(node.values[:i], append([][]byte{child.values[t-1]}, node.values[i:]...)...)
}

func (node *BTreeNode) insertNonFull(k string, v []byte) {
	i := len(node.keys) - 1
	if node.isLeaf {
		node.keys = append(node.keys, "")
		node.values = append(node.values, nil)
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

func (tree *BTree) Insert(k string, v []byte) {
	if tree.root == nil {
		tree.root = NewBTreeNode(tree.t, true)
		tree.root.keys = append(tree.root.keys, k)
		tree.root.values = append(tree.root.values, v)
	} else {
		if len(tree.root.keys) == 2*tree.t-1 {
			newRoot := NewBTreeNode(tree.t, false)
			newRoot.children = append(newRoot.children, tree.root)
			newRoot.splitChild(0, tree.t)
			i := 0
			if newRoot.keys[0] < k {
				i++
			}
			newRoot.children[i].insertNonFull(k, v)
			tree.root = newRoot
		} else {
			tree.root.insertNonFull(k, v)
		}
	}
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
	btree.Insert("jabuka", []byte{1, 2, 3})
	btree.Insert("banana", []byte{4, 5, 6})
	btree.Insert("visnja", []byte{7, 8, 9})
	btree.Insert("boronica", []byte{10, 11, 12})
	btree.Insert("steak", []byte{13, 14, 15})
	btree.Insert("jaja", []byte{16, 17, 18})
	btree.Insert("burger", []byte{19, 20, 21})

	btree.Print()
}
*/
