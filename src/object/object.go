package object

import (
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

type Object interface {
	GetObjectType() int
	GetRealData() []byte
	LockObject()
	UnLockObject()
}

type StringObj struct {
	length int
	d      []byte
	sync.Mutex
}

func CreateStringObject(data []byte) Object {
	return &StringObj{
		length: len(data),
		d:      data,
	}
}

func (o *StringObj) GetObjectType() int {
	return STRING
}

func (o *StringObj) GetRealData() []byte {
	return o.d
}

func (o *StringObj) LockObject() {
	o.Mutex.Lock()
}

func (o *StringObj) UnLockObject() {
	o.Mutex.Unlock()
}
