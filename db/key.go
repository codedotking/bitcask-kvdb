package db

import (
	rt "github.com/plar/go-adaptive-radix-tree"
)

// 基数树 key 对应的 value
type Value struct {
	dataType int
	value    interface{}
}

// 基数树 所有数据存放的地方
type RadixTree struct {
	tree rt.Tree
}

func str2bytes(str string) []byte {
	return []byte(str)
}

func (node *RadixTree) Put(key string, value Value) (oldVal interface{}, updated bool) {
	return node.tree.Insert(str2bytes(key), value)
}

func (art *RadixTree) Get(key string) interface{} {
	value, _ := art.tree.Search(str2bytes(key))
	return value
}

func (art *RadixTree) Delete(key string) (val interface{}, updated bool) {
	return art.tree.Delete(str2bytes(key))
}

func (art *RadixTree) Iterator() rt.Iterator {
	return art.tree.Iterator()
}

func (art *RadixTree) PrefixScan(prefix string, count int) (keys [][]byte) {
	cb := func(node rt.Node) bool {
		if node.Kind() != rt.Leaf {
			return true
		}
		if count <= 0 {
			return false
		}
		keys = append(keys, node.Key())
		count--
		return true
	}

	if len(prefix) == 0 {
		art.tree.ForEach(cb)
	} else {
		art.tree.ForEachPrefix(str2bytes(prefix), cb)
	}
	return
}

func (art *RadixTree) Size() int {
	return art.tree.Size()
}

func NewNode() *RadixTree {
	return &RadixTree{
		tree: rt.New(),
	}
}
