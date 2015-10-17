package godis

import (
	"bytes"
	"log"
)

const (
	After  = 1
	Before = 2
)

type List interface {
	Lput(*Object)
	Rput(*Object)
	Get(int) *Object
	Lpop() *Object
	Rpop() *Object
	Remove(*Object)
	Len() int
	Index(a *Object) int
	Insert(p, i int, a *Object)
	Clear()
}

type node struct {
	value *Object
	prev  *node
	next  *node
}

type doublelinkedlist struct {
	n    int
	head *node
	tail *node
}

func NewList() List {
	return &doublelinkedlist{}
}

func createNode(a *Object) *node {
	if a == nil {
		return nil
	}
	return &node{
		value: a,
	}
}

func eq(n1 *node, n2 *node) bool {
	var obj1 *Object = n1.value
	var obj2 *Object = n2.value
	return bytes.Equal(obj1.GetBuffer(), obj2.GetBuffer())
}

func (self *doublelinkedlist) findNodeByIndex(i int) *node {
	var index int = 0
	for node := self.head; node != nil; node = node.next {
		if i == index {
			return node
		}
		index++
	}
	return nil
}

func (self *doublelinkedlist) findNodeByNode(n *node) *node {
	for node := self.head; node != nil; node = node.next {
		if eq(node, n) {
			return node
		}
	}
	return nil
}

func (self *doublelinkedlist) Lput(a *Object) {
	node := createNode(a)
	if node == nil {
		return
	}
	if self.n > 0 {
		node.next = self.head
		node.prev = nil
		self.head.prev = node
		self.head = node
	} else {
		self.head = node
		self.tail = node
		node.prev = nil
		node.next = nil
	}
	self.n++
}

func (self *doublelinkedlist) Rput(a *Object) {
	node := createNode(a)
	if self.n > 0 {
		node.prev = self.tail
		node.next = nil
		self.tail.next = node
		self.tail = node
	} else {
		self.head = node
		self.tail = node
		node.prev = nil
		node.next = nil
	}
	self.n++
}

func (self *doublelinkedlist) Index(a *Object) int {
	index := -1
	witchnode := createNode(a)
	for node := self.head; node != nil; node = node.next {
		index++
		if eq(node, witchnode) {
			return index
		}
	}
	return -1
}

func (l *doublelinkedlist) Get(i int) *Object {
	return l.findNodeByIndex(i).value
}

func (self *doublelinkedlist) Lpop() *Object {
	if self.n == 0 {
		return nil
	}
	node := self.head
	self.n--
	if self.n > 0 {
		self.head = node.next
		self.head.prev = nil
	} else {
		self.head = nil
		self.tail = nil
	}
	node.next = nil
	node.prev = nil
	return node.value
}

func (self *doublelinkedlist) Rpop() *Object {
	if self.n == 0 {
		return nil
	}
	node := self.tail
	self.n--
	if self.n > 0 {
		self.tail = node.prev
		self.tail.next = nil
	} else {
		self.tail = nil
		self.head = nil
	}
	node.next = nil
	node.prev = nil
	return node.value
}

func (self *doublelinkedlist) Remove(a *Object) {
	node := self.findNodeByNode(createNode(a))
	if node == nil {
		return
	}
	if node.prev != nil {
		node.prev.next = node.next
	} else {
		self.head = node.next
	}
	if node.next != nil {
		node.next.prev = node.prev
	} else {
		self.tail = node.prev
	}
	if node.value != nil {
		node.value = nil
	}
	self.n--
}

func (l *doublelinkedlist) Len() int {
	return l.n
}

func (l *doublelinkedlist) Insert(p, i int, a *Object) {
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
		newnode.prev = node
		newnode.next = node.next
		if node.next == nil {
			l.tail = newnode
		} else {
			node.next.prev = newnode
		}
		node.next = newnode
	} else if p == Before {
		newnode.prev = node.prev
		newnode.next = node
		if node.prev == nil {
			l.head = newnode
		} else {
			node.prev.next = newnode
		}
		node.prev = newnode
	}
	l.n++
}

func (l *doublelinkedlist) Clear() {
	l.n = 0
	l.head = nil
	l.tail = nil
}
