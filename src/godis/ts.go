package godis

import (
	ds "data_struct"
	"db"
	"errors"
	"log"
	"store"
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
	datalog      *store.DataLog
	tsLog        *store.TsLog
	status       uint8
	position     *store.RecordPosition
}

type TsRecord struct {
	TsrId       int
	SavePointId int
	Op          uint8
	Key         []byte
	Value       []byte
	Dbptr       *db.DB
	Position    *store.RecordPosition
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
		datalog: godis.Dl,
		tsLog:   godis.Tl,
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
	log.Println("LockDB", db.Lock)
	return db.Lock.TryLock(ts.timeout, ts.TsId)
}

func (ts *Ts) UnLockDB(db *db.DB) {
	db.Lock.Cancel()
}

func (ts *Ts) GetDBKeys(db *db.DB) ds.List {
	list := ds.NewList()
	for key, _ := range db.Data {
		list.Put(ds.CreateObject([]byte(key), ds.BIN, ts.TsId))
	}
	for key, _ := range ts.magicDB {
		list.Put(ds.CreateObject([]byte(key), ds.BIN, ts.TsId))
	}
	return list
}
func (ts *Ts) SetDBKey(db *db.DB, t uint8, key []byte, value []byte) {
	var err error
	tsr := NewTsRecord(AddDbKey)
	if origObj := db.GetDbObj(key); origObj != nil {
		log.Println("SetDBKey() origObj:", origObj)
		tsr.Position = ts.tsLog.Put(db, origObj.GetObjectType(), key,
			origObj.GetBuffer())
		if tsr.Position == nil {
			log.Panicln(err)
		}
	}
	tsr.Key = key
	tsr.Dbptr = db
	ts.AddTsRecord(tsr)
	ts.setMagicDb(key, ds.CreateObject(value, t, ts.TsId))
}
func (ts *Ts) DeleteDBKey(db *db.DB, key []byte) {
	var err error
	tsr := NewTsRecord(DeleteDbKey)
	tsr.Key = key
	tsr.Dbptr = db
	if origObj := db.GetDbObj(key); origObj != nil {
		tsr.Position = ts.tsLog.Put(db, origObj.GetObjectType(), key,
			origObj.GetBuffer())
		if tsr.Position == nil {
			log.Panicln(err)
		}
	}
	ts.AddTsRecord(tsr)
	obj := ts.getMagicDb(key)
	if obj != nil {
		ts.delMagicDb(key)
	} else {
		obj = db.GetDbObj(key)
		if obj != nil {
			ts.setMagicDb(key, obj)
			db.DeleteDbObj(key)
		}
	}
}

func (ts *Ts) GetDBKey(db *db.DB, key []byte) *ds.Object {
	if obj := db.GetDbObj(key); obj != nil {
		return obj
	} else {
		obj := ts.getMagicDb(key)
		if obj != nil {
			return obj
		}
	}
	return nil
}

func (ts *Ts) doCommit() {
	var (
		tsr *TsRecord
		ok  bool
	)
	for e := ts.tsrList.GetFirstNode(); e != nil; e = e.Next {
		if tsr, ok = e.Value.(*TsRecord); !ok {
			continue
		}
		ts.commitATsr(tsr)
		if tsr.Dbptr != nil {
			tsr.Dbptr.Lock.Cancel()
		}
	}
}

func (ts *Ts) storeTsr() {
	var tsr *TsRecord
	var ok bool
	list := ds.NewList()
	for e := ts.tsrList.GetFirstNode(); e != nil; e = e.Next {
		if tsr, ok = e.Value.(*TsRecord); !ok {
			continue
		}
		if tsr.Position != nil {
			list.Put(tsr.Position)
		}
	}
	if list.Len() == 0 {
		return
	}
	ts.position = ts.tsLog.PutAMeta(store.Commit, ts.TsId, list)
	if ts.position == nil {
		log.Panicln("storeTsr()", list.Len())
	}
}

func (ts *Ts) Commit() error {
	log.Println("tsr Len()", ts.tsrList.Len())
	if ts.tsrList.Len() == 0 {
		return err_not_found_ts
	}
	log.Println("开始commmit")
	printTsrArray(ts.tsrList)
	// 保存事务日志
	ts.storeTsr()
	// 打上Commit标志
	if ts.position != nil {
		ts.tsLog.SetTsStatus(ts.position, store.Commit)
	}
	// 开始提交
	ts.doCommit()
	log.Println("debug")
	time.Sleep(time.Second * 10)
	// 打上Commited标志
	if ts.position != nil {
		ts.tsLog.SetTsStatus(ts.position, store.Committed)
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

func (ts *Ts) subTsrListBySavePoint(savepoint int) ds.List {
	var i int = 0
	var tsr *TsRecord
	var ok bool
	var l ds.List = ts.tsrList
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
		rollbacklist = ts.subTsrListBySavePoint(savepoint)
	} else {
		rollbacklist = ts.tsrList
	}
	printTsrArray(rollbacklist)
	printTsrArray(ts.tsrList)
	if rollbacklist.Len() == 0 {
		return nil
	}
	for e := rollbacklist.GetTailNode(); e != nil; e = e.Prev {
		if tsr, ok = e.Value.(*TsRecord); !ok {
			continue
		}
		ts.rollBackATsr(tsr)
		if tsr.Dbptr != nil {
			tsr.Dbptr.Lock.Cancel()
		}
	}
	return nil
}

func (ts *Ts) rollBackATsr(tsr *TsRecord) {
	switch tsr.Op {
	case DeleteDbKey:
		ts.rollbackDbDel(tsr.Dbptr, tsr.Key)
	case AddDbKey:
		ts.rollbackDbAdd(tsr.Dbptr, tsr.Key)
	case SavePoint:
		ts.curSavePoint--
	}
}

func (ts *Ts) commitATsr(tsr *TsRecord) {
	switch tsr.Op {
	case DeleteDbKey:
		ts.commitDbDel(tsr.Dbptr, tsr.Key)
	case AddDbKey:
		ts.commitDbAdd(tsr.Dbptr, tsr.Key)
	}
}

func (ts *Ts) rollbackDbDel(db *db.DB, key []byte) {
	obj := ts.getMagicDb(key)
	if obj != nil {
		db.SetDbObj(key, obj)
	}
	ts.delMagicDb(key)
}

func (ts *Ts) commitDbDel(db *db.DB, key []byte) {
	obj := ts.getMagicDb(key)
	if obj != nil {
		ts.datalog.PutKeyValue(db, key, store.Del, obj)
		delete(ts.magicDB, string(key))
	}
}

func (ts *Ts) rollbackDbAdd(db *db.DB, key []byte) {
	ts.delMagicDb(key)
}

func (ts *Ts) commitDbAdd(db *db.DB, key []byte) {
	obj := ts.getMagicDb(key)
	log.Println("Obj", obj)
	if obj != nil {
		ts.datalog.PutKeyValue(db, key, store.None, obj)
		db.SetDbObj(key, obj)
	}
}

func (ts *Ts) setMagicDb(key []byte, value *ds.Object) {
	ts.magicDB[string(key)] = value
}

func (ts *Ts) getMagicDb(key []byte) *ds.Object {
	obj, ok := ts.magicDB[string(key)]
	if ok {
		return obj
	}
	return nil
}

func (ts *Ts) delMagicDb(key []byte) *ds.Object {
	obj := ts.getMagicDb(key)
	delete(ts.magicDB, string(key))
	return obj
}
