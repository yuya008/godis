package db

import (
	"fmt"
	"object"
	"sync"
)

type DB struct {
	// 数据库ID
	Id int
	// 数据库名称
	DbName string
	// 数据库键数量
	KeyN uint64
	// 数据
	Data map[string]object.Object
	// 数据库锁
	sync.Mutex
}

func InitDB(id int, db *DB) {
	db.Id = id
	db.DbName = fmt.Sprintf("db%d", id)
	db.Data = make(map[string]object.Object)
}

func (db *DB) DelKey(key string) {
	delete(db.Data, key)
}
