package wal

import (
	"os"
)

type KV struct {
	Index int64
	Value *os.File
}

type Node struct {
	Data KV
	Next *Node
}

type OrderedLinkedList struct {
	Head *Node
}

func NewOrderedLinkedList() *OrderedLinkedList {
	return &OrderedLinkedList{}
}

func (oll *OrderedLinkedList) Insert(k int64, v *os.File) {
	newNode := &Node{Data: KV{k, v}}

	if oll.Head == nil || oll.Head.Data.Index >= k {
		newNode.Next = oll.Head
		oll.Head = newNode
		return
	}

	current := oll.Head
	for current.Next != nil && current.Next.Data.Index < k {
		current = current.Next
	}

	newNode.Next = current.Next
	current.Next = newNode
}

func (oll *OrderedLinkedList) Find(index int64) *os.File {
	current := oll.Head
	var prev *Node

	for current != nil && current.Data.Index < index {
		prev = current
		current = current.Next
	}

	if current != nil && current.Data.Index == index {
		return current.Data.Value
	}

	if prev != nil {
		return prev.Data.Value
	}

	return nil
}
