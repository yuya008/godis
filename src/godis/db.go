package godis

import (
	ds "data_struct"
	"fmt"
)

type DB struct {
	// 数据库ID
	Id int
	// 数据库名称
	DbName string
	// 数据库键数量
	KeyN uint64
	// 数据
	Data map[string]*ds.Object
	// 读写锁
	lock *TsLock
}

func InitDB(id int, db *DB) {
	db.Id = id
	db.DbName = fmt.Sprintf("db%d", id)
	db.Data = make(map[string]*ds.Object)
	db.lock = NewTsLock()
}

func (db *DB) DeleteKey(key string) {
	delete(db.Data, key)
}
