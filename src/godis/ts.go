package godis

import (
	ds "data_struct"
	"db"
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
	SavePoint
)

var (
	err_ts_lock_timeout     = errors.New("[error] lock object timeout")
	err_no_start_ts         = errors.New("[error] no open transaction!")
	err_not_found_savepoint = errors.New("[error] not found savepoint!")
	err_not_found_ts        = errors.New("[error] not found transaction!")
	err_rollback_fail       = errors.New("[error] rollback fail!")
	err_commit_back         = errors.New("[error] commit fail!")
)

// 全局事务编号
var TsGlobalId uint64 = 0

type Ts struct {
	TsId     uint64
	CurTsrId int
	timeout  time.Duration
	tsrList  ds.List
	magicDB  map[string]*ds.Object
	// magicHT *HashTable
	// magicHT *List
	curSavePoint int
}

type TsRecord struct {
	TsrId       int
	SavePointId int
	Op          uint8
	Key         string
	Value       []byte
	Dbptr       *db.DB
}

func NewTsRecord(op uint8) *TsRecord {
	return &TsRecord{
		Op: op,
	}
}

func NewTs(godis *Godis) *Ts {
	return &Ts{
		TsId:    atomic.AddUint64(&TsGlobalId, 1),
		timeout: godis.Tstimeout,
		tsrList: ds.NewList(),
		magicDB: make(map[string]*ds.Object),
	}
}

func (ts *Ts) AddTsRecord(tsr *TsRecord) {
	tsr.TsrId = ts.CurTsrId
	ts.tsrList.Put(tsr)
	ts.CurTsrId++
}

func (ts *Ts) AddSavePoint() {
	sp := NewTsRecord(SavePoint)
	sp.SavePointId = ts.curSavePoint
	ts.curSavePoint++
	ts.AddTsRecord(sp)
}

func (ts *Ts) RlockDB(db *db.DB) bool {
	return db.Lock.TryRLock(ts.timeout, ts.TsId)
}

func (ts *Ts) LockDB(db *db.DB) bool {
	return db.Lock.TryLock(ts.timeout, ts.TsId)
}

func (ts *Ts) GetDBKeys(db *db.DB) ds.List {
	list := ds.NewList()
	for key, _ := range db.Data {
		list.Put(ds.CreateObject([]byte(key), ts.TsId))
	}
	for key, _ := range ts.magicDB {
		list.Put(ds.CreateObject([]byte(key), ts.TsId))
	}
	return list
}
func (ts *Ts) SetDBKey(db *db.DB, key string, value []byte) {
	tsr := NewTsRecord(AddDbKey)
	tsr.Key = key
	tsr.Dbptr = db
	ts.AddTsRecord(tsr)
	ts.magicDB[key] = ds.CreateObject(value, ts.TsId)
}
func (ts *Ts) DeleteDBKey(db *db.DB, key string) {
	tsr := NewTsRecord(DeleteDbKey)
	tsr.Key = key
	tsr.Dbptr = db
	ts.AddTsRecord(tsr)

	obj, ok := ts.magicDB[key]
	if ok {
		delete(ts.magicDB, key)
	} else {
		obj = db.GetDbKey(key)
		if obj != nil {
			ts.magicDB[key] = obj
			db.DeleteKey(key)
		}
	}
}

func (ts *Ts) GetDBKey(db *db.DB, name string) *ds.Object {
	if obj := db.GetDbKey(name); obj != nil {
		return obj
	} else {
		obj, ok := ts.magicDB[name]
		if ok {
			return obj
		}
	}
	return nil
}

func (ts *Ts) Commit() error {
	var tsr *TsRecord
	var ok bool
	log.Println("tsr Len()", ts.tsrList.Len())
	if ts.tsrList.Len() == 0 {
		return err_not_found_ts
	}
	log.Println("开始commmit")
	printTsrArray(ts.tsrList)
	for e := ts.tsrList.GetFirstNode(); e != nil; e = e.Next {
		if tsr, ok = e.Value.(*TsRecord); !ok {
			continue
		}
		commitATsr(ts, tsr)
		if tsr.Dbptr != nil {
			tsr.Dbptr.Lock.Cancel()
		}
	}
	return nil
}

func printTsrArray(tsrList ds.List) {
	var tsr *TsRecord
	var ok bool
	log.Println("----------------")
	for e := tsrList.GetFirstNode(); e != nil; e = e.Next {
		if tsr, ok = e.Value.(*TsRecord); !ok {
			continue
		}
		log.Println(tsr)
	}
	log.Println("----------------")
}

func subTsrListBySavePoint(l ds.List, savepoint int) ds.List {
	var i int = 0
	var tsr *TsRecord
	var ok bool
	for e := l.GetFirstNode(); e != nil; e = e.Next {
		if tsr, ok = e.Value.(*TsRecord); !ok {
			continue
		}
		if tsr.Op == SavePoint && savepoint == tsr.SavePointId {
			break
		}
		i++
	}
	if list := l.SubList(i, l.Len()); list != nil {
		for e := list.GetFirstNode(); e != nil; e = e.Next {
			l.Remove(e.Value)
		}
		return list
	}
	return l
}

func (ts *Ts) RollBack(savepoint int) error {
	var tsr *TsRecord
	var ok bool
	var rollbacklist ds.List
	if ts.tsrList.Len() == 0 {
		return err_not_found_ts
	}
	if savepoint >= ts.curSavePoint {
		return err_not_found_savepoint
	}
	printTsrArray(ts.tsrList)
	if savepoint >= 0 {
		rollbacklist = subTsrListBySavePoint(ts.tsrList, savepoint)
	} else {
		rollbacklist = ts.tsrList
	}
	printTsrArray(rollbacklist)
	printTsrArray(ts.tsrList)
	for e := rollbacklist.GetTailNode(); e != nil; e = e.Prev {
		if tsr, ok = e.Value.(*TsRecord); !ok {
			continue
		}
		rollBackATsr(ts, tsr)
		if tsr.Dbptr != nil {
			tsr.Dbptr.Lock.Cancel()
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

func rollbackDbDel(ts *Ts, db *db.DB, key string) {
	obj, ok := ts.magicDB[key]
	if ok {
		db.SetDbKey(key, obj)
	}
	delete(ts.magicDB, key)
}

func commitDbDel(ts *Ts, db *db.DB, key string) {
	delete(ts.magicDB, key)
}

func rollbackDbAdd(ts *Ts, db *db.DB, key string) {
	delete(ts.magicDB, key)
}

func commitDbAdd(ts *Ts, db *db.DB, key string) {
	obj, ok := ts.magicDB[key]
	log.Println("Obj", obj, ok)
	if ok {
		db.SetDbKey(key, obj)
	}
}
