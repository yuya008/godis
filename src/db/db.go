package db

import (
	ds "data_struct"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
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

func (db *DB) DeleteDbObj(key []byte) {
	delete(db.Data, string(key))
}

func (db *DB) SetDbObj(key []byte, obj *ds.Object) {
	db.Data[string(key)] = obj
}

func (db *DB) GetDbObj(key []byte) *ds.Object {
	obj, ok := db.Data[string(key)]
	if ok {
		return obj
	}
	return nil
}

// |keylen|key|vallen|value|
func (db *DB) LoadDbObjFromReader(reader io.Reader, objtype uint8) error {
	var (
		argsN  uint16
		i      uint16
		keylen uint64
		vallen uint64
	)
	err := binary.Read(reader, binary.BigEndian, &argsN)
	if err != nil {
		return err
	}
	if argsN%2 != 0 {
		return errors.New("argsN fail!")
	}
	for i = 0; i < argsN; i++ {
		err = binary.Read(reader, binary.BigEndian, &keylen)
		if err != nil {
			return err
		}
		keybuf := make([]byte, keylen)
		_, err = reader.Read(keybuf)
		if err != nil {
			return err
		}
		i++
		err = binary.Read(reader, binary.BigEndian, &vallen)
		if err != nil {
			return err
		}
		valbuf := make([]byte, vallen)
		_, err = reader.Read(valbuf)
		if err != nil {
			return err
		}
		i++
		db.Data[string(keybuf)] = ds.CreateObject(valbuf, objtype, 0)
	}
	log.Println(db.Data)
	return nil
}
