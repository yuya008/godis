package godis

import (
	golist "container/list"
	ds "data_struct"
	"errors"
	"log"
	"sync/atomic"
	"time"
)

const (
	Overide = iota
	DeleteDbKey
	AddDbKey
	Lookup
)

var (
	err_ts_lock_timeout = errors.New("[error] lock object timeout")
)

// 全局事务编号
var TsGlobalId uint64 = 0

type Ts struct {
	TsId     uint64
	CurTsrId int
	timeout  time.Duration
	tsrArray *golist.List
	magicDB  map[string]*ds.Object
	// magicHT *HashTable
	// magicHT *List
}

type TsRecord struct {
	TsrId int
	Op    uint8
	Key   string
	Value []byte
	Dbptr *DB
}

func NewTsRecord(op uint8) *TsRecord {
	return &TsRecord{
		Op: op,
	}
}

func NewTs(godis *Godis) *Ts {
	return &Ts{
		TsId:     atomic.AddUint64(&TsGlobalId, 1),
		timeout:  godis.Tstimeout,
		tsrArray: golist.New(),
		magicDB:  make(map[string]*ds.Object),
	}
}

func (ts *Ts) AddTsRecord(tsr *TsRecord) {
	tsr.TsrId = ts.CurTsrId
	ts.tsrArray.PushBack(tsr)
	ts.CurTsrId++
}

func (ts *Ts) RlockDB(db *DB) bool {
	return db.lock.TryRLock(ts.timeout, ts.TsId)
}

func (ts *Ts) LockDB(db *DB) bool {
	return db.lock.TryLock(ts.timeout, ts.TsId)
}

func (ts *Ts) GetDBKeys(db *DB) *golist.List {
	list := golist.New()
	for key, _ := range db.Data {
		list.PushBack(ds.CreateObject([]byte(key), ts.TsId))
	}
	for key, _ := range ts.magicDB {
		list.PushBack(ds.CreateObject([]byte(key), ts.TsId))
	}
	return list
}
func (ts *Ts) SetDBKey(db *DB, key string, value []byte) {
	tsr := NewTsRecord(AddDbKey)
	tsr.Key = key
	tsr.Dbptr = db
	ts.AddTsRecord(tsr)
	ts.magicDB[key] = ds.CreateObject(value, ts.TsId)
}
func (ts *Ts) DeleteDBKey(db *DB, key string) {
	tsr := NewTsRecord(DeleteDbKey)
	tsr.Key = key
	tsr.Dbptr = db
	ts.AddTsRecord(tsr)

	obj, ok := ts.magicDB[key]
	if ok {
		delete(ts.magicDB, key)
	} else {
		obj, ok = db.Data[key]
		if ok {
			ts.magicDB[key] = obj
			delete(db.Data, key)
		}
	}
}

func (ts *Ts) GetDBKey(db *DB, name string) *ds.Object {
	if obj, ok := db.Data[name]; ok {
		return obj
	} else {
		obj, ok = ts.magicDB[name]
		if ok {
			return obj
		}
	}
	return nil
}

func (ts *Ts) Commit() error {
	var tsr *TsRecord
	var ok bool
	log.Println("tsr Len()", ts.tsrArray.Len())
	if ts.tsrArray.Len() == 0 {
		return nil
	}
	log.Println("开始commmit")
	for e := ts.tsrArray.Front(); e != nil; e = e.Next() {
		if tsr, ok = e.Value.(*TsRecord); !ok {
			continue
		}
		commitATsr(ts, tsr)
	}
	// 释放锁
	for e := ts.tsrArray.Back(); e != nil; e = e.Prev() {
		if tsr, ok = e.Value.(*TsRecord); !ok {
			continue
		}
		if tsr.Dbptr != nil {
			tsr.Dbptr.lock.Cancel(ts.TsId)
		}
	}
	return nil
}

func (ts *Ts) RollBack() error {
	var tsr *TsRecord
	var ok bool
	if ts.tsrArray.Len() == 0 {
		return nil
	}
	for e := ts.tsrArray.Back(); e != nil; e = e.Prev() {
		if tsr, ok = e.Value.(*TsRecord); !ok {
			continue
		}
		rollBackATsr(ts, tsr)
	}
	// 释放锁
	for e := ts.tsrArray.Back(); e != nil; e = e.Prev() {
		if tsr, ok = e.Value.(*TsRecord); !ok {
			continue
		}
		if tsr.Dbptr != nil {
			tsr.Dbptr.lock.Cancel(ts.TsId)
		}
	}
	return nil
}

func rollBackATsr(ts *Ts, tsr *TsRecord) {
	switch tsr.Op {
	case DeleteDbKey:
		rollbackDbDel(ts, tsr.Dbptr, tsr.Key)
	case AddDbKey:
		rollbackDbAdd(ts, tsr.Dbptr, tsr.Key)
	}
}

func commitATsr(ts *Ts, tsr *TsRecord) {
	switch tsr.Op {
	case DeleteDbKey:
		commitDbDel(ts, tsr.Dbptr, tsr.Key)
	case AddDbKey:
		commitDbAdd(ts, tsr.Dbptr, tsr.Key)
	}
}

func rollbackDbDel(ts *Ts, db *DB, key string) {
	obj, ok := ts.magicDB[key]
	if ok {
		db.Data[key] = obj
	}
	delete(ts.magicDB, key)
}

func commitDbDel(ts *Ts, db *DB, key string) {
	delete(ts.magicDB, key)
}

func rollbackDbAdd(ts *Ts, db *DB, key string) {
	delete(ts.magicDB, key)
}

func commitDbAdd(ts *Ts, db *DB, key string) {
	obj, ok := ts.magicDB[key]
	log.Println("Obj", obj, ok)
	if ok {
		db.Data[key] = obj
	}
}
