package trees

import (
	"fmt"
)

type BTreeNode struct {
	isLeaf   bool
	keys     []int
	children []*BTreeNode
	t        int
}

func NewBTreeNode(t int, isLeaf bool) *BTreeNode {
	return &BTreeNode{
		isLeaf:   isLeaf,
		keys:     make([]int, 0),
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
	child.keys = child.keys[:t-1]

	if !child.isLeaf {
		newChild.children = append(newChild.children, child.children[t:]...)
		child.children = child.children[:t]
	}

	node.children = append(node.children[:i+1], append([]*BTreeNode{newChild}, node.children[i+1:]...)...)
	node.keys = append(node.keys[:i], append([]int{child.keys[t-1]}, node.keys[i:]...)...)
}

// Ubacivanje ključa u nepuni čvor
func (node *BTreeNode) insertNonFull(k int) {
	i := len(node.keys) - 1
	if node.isLeaf {
		node.keys = append(node.keys, 0)
		for i >= 0 && node.keys[i] > k {
			node.keys[i+1] = node.keys[i]
			i--
		}
		node.keys[i+1] = k
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
		node.children[i].insertNonFull(k)
	}
}

// Ubacivanje ključa u stablo
func (tree *BTree) Insert(k int) {
	if tree.root == nil {
		tree.root = NewBTreeNode(tree.t, true)
		tree.root.keys = append(tree.root.keys, k)
	} else {
		if len(tree.root.keys) == 2*tree.t-1 {
			newRoot := NewBTreeNode(tree.t, false)
			newRoot.children = append(newRoot.children, tree.root)
			newRoot.splitChild(0, tree.t)
			i := 0
			if newRoot.keys[0] < k {
				i++
			}
			newRoot.children[i].insertNonFull(k)
			tree.root = newRoot
		} else {
			tree.root.insertNonFull(k)
		}
	}
}

func (node *BTreeNode) Print(level int) {
	fmt.Printf("Level %d Keys: %v\n", level, node.keys)
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

// testiranje;;; nisam završio ima par problema!!!
func main() {
	btree := NewBTree(3)
	btree.Insert(10)
	btree.Insert(20)
	btree.Insert(5)
	btree.Insert(6)
	btree.Insert(12)
	btree.Insert(30)
	btree.Insert(7)
	btree.Insert(17)

	btree.Print()
}

*/