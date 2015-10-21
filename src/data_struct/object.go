package data_struct

import (
	"sync"
)

const (
	NonTs = 0
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
	// 二进制类型
	BIN = 6
)

type Object struct {
	objType   uint8
	objBuffer []byte
	Hash      map[string]*Object
	List      List
	sync.RWMutex
	tsid uint64
}

func CreateStringObject(data []byte, tsid uint64) *Object {
	return &Object{
		objBuffer: data,
		objType:   STRING,
		tsid:      tsid,
	}
}

func CreateObject(data []byte, tsid uint64) *Object {
	return &Object{
		objBuffer: data,
		objType:   BIN,
		tsid:      tsid,
	}
}

func CreateObjectFromString(str string, tsid uint64) *Object {
	return &Object{
		objBuffer: []byte(str),
		objType:   BIN,
		tsid:      tsid,
	}
}

func CreateListObject(tsid uint64) *Object {
	return &Object{
		objType: LIST,
		List:    NewList(),
		tsid:    tsid,
	}
}

func (o *Object) GetObjectType() uint8 {
	return o.objType
}

func (o *Object) GetBuffer() []byte {
	return o.objBuffer
}

func (o *Object) GetTsId() uint64 {
	return o.tsid
}

func (o *Object) SetTsId(id uint64) {
	o.tsid = id
}
