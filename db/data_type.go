package db

import "sync"

type (
	strIndex struct {
		mu   *sync.RWMutex
		tree *RadixTree
	}
)

// NewStrIndex 实例化一个 strIndex
func NewStrIndex() *strIndex {
	return &strIndex{mu: new(sync.RWMutex), tree: NewRadixTree()}
}
