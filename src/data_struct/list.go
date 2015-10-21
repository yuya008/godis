package data_struct

import (
	"log"
)

const (
	After  = 1
	Before = 2
)

type List interface {
	Lput(interface{})
	Rput(interface{})
	Get(int) interface{}
	Lpop() interface{}
	Rpop() interface{}
	Put(interface{})
	Pop() interface{}
	Remove(interface{})
	Len() int
	Index(a interface{}) int
	SubList(int, int) List
	Insert(p, i int, a interface{})
	Clear()
	GetFirstNode() *node
	GetTailNode() *node
}

type node struct {
	Value interface{}
	Prev  *node
	Next  *node
}

type doublelinkedlist struct {
	n    int
	Head *node
	Tail *node
	Eq   func(*node, *node) bool
}

func NewList() List {
	return &doublelinkedlist{
		Eq: eq,
	}
}

func createNode(a interface{}) *node {
	if a == nil {
		return nil
	}
	return &node{
		Value: a,
	}
}

func eq(n1 *node, n2 *node) bool {
	return n1.Value == n2.Value
}

func (self *doublelinkedlist) findNodeByIndex(i int) *node {
	var index int = 0
	for node := self.Head; node != nil; node = node.Next {
		if i == index {
			return node
		}
		index++
	}
	return nil
}

func (self *doublelinkedlist) GetFirstNode() *node {
	return self.Head
}

func (self *doublelinkedlist) GetTailNode() *node {
	return self.Tail
}

func (self *doublelinkedlist) findNodeByNode(n *node) *node {
	for node := self.Head; node != nil; node = node.Next {
		if self.Eq(node, n) {
			return node
		}
	}
	return nil
}

func (self *doublelinkedlist) Lput(a interface{}) {
	node := createNode(a)
	if node == nil {
		return
	}
	if self.n > 0 {
		node.Next = self.Head
		node.Prev = nil
		self.Head.Prev = node
		self.Head = node
	} else {
		self.Head = node
		self.Tail = node
		node.Prev = nil
		node.Next = nil
	}
	self.n++
}

func (self *doublelinkedlist) Rput(a interface{}) {
	node := createNode(a)
	if self.n > 0 {
		node.Prev = self.Tail
		node.Next = nil
		self.Tail.Next = node
		self.Tail = node
	} else {
		self.Head = node
		self.Tail = node
		node.Prev = nil
		node.Next = nil
	}
	self.n++
}

func (self *doublelinkedlist) Put(a interface{}) {
	self.Rput(a)
}

func (self *doublelinkedlist) Pop() interface{} {
	return self.Lpop()
}

func (self *doublelinkedlist) Index(a interface{}) int {
	index := -1
	witchnode := createNode(a)
	for node := self.Head; node != nil; node = node.Next {
		index++
		if self.Eq(node, witchnode) {
			return index
		}
	}
	return -1
}

func (l *doublelinkedlist) Get(i int) interface{} {
	return l.findNodeByIndex(i).Value
}

func (self *doublelinkedlist) Lpop() interface{} {
	if self.n == 0 {
		return nil
	}
	node := self.Head
	self.n--
	if self.n > 0 {
		self.Head = node.Next
		self.Head.Prev = nil
	} else {
		self.Head = nil
		self.Tail = nil
	}
	node.Next = nil
	node.Prev = nil
	return node.Value
}

func (self *doublelinkedlist) Rpop() interface{} {
	if self.n == 0 {
		return nil
	}
	node := self.Tail
	self.n--
	if self.n > 0 {
		self.Tail = node.Prev
		self.Tail.Next = nil
	} else {
		self.Tail = nil
		self.Head = nil
	}
	node.Next = nil
	node.Prev = nil
	return node.Value
}

func (self *doublelinkedlist) Remove(a interface{}) {
	node := self.findNodeByNode(createNode(a))
	if node == nil {
		return
	}
	if node.Prev != nil {
		node.Prev.Next = node.Next
	} else {
		self.Head = node.Next
	}
	if node.Next != nil {
		node.Next.Prev = node.Prev
	} else {
		self.Tail = node.Prev
	}
	if node.Value != nil {
		node.Value = nil
	}
	self.n--
}

func (l *doublelinkedlist) Len() int {
	return l.n
}

func (l *doublelinkedlist) Insert(p, i int, a interface{}) {
	if p != After && p != Before {
		return
	}
	node := l.findNodeByIndex(i)
	if node == nil {
		log.Println(i)
		return
	}
	newnode := createNode(a)
	if newnode == nil {
		return
	}
	if p == After {
		newnode.Prev = node
		newnode.Next = node.Next
		if node.Next == nil {
			l.Tail = newnode
		} else {
			node.Next.Prev = newnode
		}
		node.Next = newnode
	} else if p == Before {
		newnode.Prev = node.Prev
		newnode.Next = node
		if node.Prev == nil {
			l.Head = newnode
		} else {
			node.Prev.Next = newnode
		}
		node.Prev = newnode
	}
	l.n++
}

func (l *doublelinkedlist) Clear() {
	l.n = 0
	l.Head = nil
	l.Tail = nil
}

func (self *doublelinkedlist) SubList(fromIndex int, toIndex int) List {
	if fromIndex < 0 || fromIndex >= toIndex || toIndex < 0 {
		return nil
	}
	list := NewList()
	var index int
	for node := self.Head; node != nil; node = node.Next {
		if index >= fromIndex {
			list.Put(node.Value)
		}
		index++
		if index == toIndex {
			break
		}
	}
	return list
}
