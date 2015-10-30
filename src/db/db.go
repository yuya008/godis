package db

import (
	ds "data_struct"
	"fmt"
	"tslock"
)

type DB struct {
	// 数据库ID
	Id uint16
	// 数据库名称
	DbName string
	// 数据库键数量
	KeyN uint64
	// 数据
	Data map[string]*ds.Object
	// 读写锁
	Lock *tslock.TsLock
}

func InitDB(id uint16, db *DB) {
	db.Id = id
	db.DbName = fmt.Sprintf("db%d", id)
	db.Data = make(map[string]*ds.Object)
	db.Lock = tslock.NewTsLock()
}

func (db *DB) DeleteKey(key []byte) {
	delete(db.Data, string(key))
}

func (db *DB) SetDbKey(key []byte, obj *ds.Object) {
	db.Data[string(key)] = obj
}

func (db *DB) GetDbKey(key []byte) *ds.Object {
	obj, ok := db.Data[string(key)]
	if ok {
		return obj
	}
	return nil
}
