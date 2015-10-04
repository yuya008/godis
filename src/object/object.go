package object

import (
	"container/list"
	"sync"
)

const (
	// 字符串类型对象
	STRING = 1
	// 列表类型对象
	LIST = 2
	// 哈希表类型对象
	HASH = 3
	// 队列类型对象
	QUEUE = 4
	// 集合类型
	SET = 5
)

type Object struct {
	objType   int
	objLen    uint64
	objBuffer []byte
	objHash   map[string]*Object
	objList   list.List
	sync.Mutex
}

func CreateStringObject(data []byte) *Object {
	return &Object{
		objBuffer: data,
		objType:   STRING,
	}
}

func (o *Object) GetObjectType() int {
	return o.objType
}

func (o *Object) GetBuffer() []byte {
	return o.objBuffer
}

func (o *Object) LockObject() {
	o.Lock()
}

func (o *Object) UnLockObject() {
	o.Unlock()
}
