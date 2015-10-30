package godis

import (
	ds "data_struct"
	"db"
	"log"
	"strconv"
)

func cmdBegin(c *Client) {
	c.AutoCommit = false
	reply(c, success, nil)
}

func cmdUse(c *Client) {
	args, err := getArgs(c)
	if err != nil {
		reply(c, err.Error(), nil)
		return
	}
	whitchdb, err := strconv.Atoi(string(args[0]))
	if err != nil {
		reply(c, err_cmd.Error(), nil)
		return
	}
	dbs := c.godis.Dbs
	if whitchdb < 0 || whitchdb >= len(dbs) {
		reply(c, err_db_not_found.Error(), nil)
		return
	}
	c.CurDB = &dbs[whitchdb]
	reply(c, success, nil)
}

func getCurrentDb(ts *Ts, db *db.DB) (*db.DB, error) {
	if !ts.RlockDB(db) {
		return nil, err_ts_lock_timeout
	}
	tsr := NewTsRecord(Lookup)
	tsr.Dbptr = db
	ts.AddTsRecord(tsr)
	return db, nil
}

func cmdKeys(c *Client) {
	db, err := getCurrentDb(c.ts, c.CurDB)
	if err != nil {
		reply(c, err.Error(), nil)
		return
	}
	keys := c.ts.GetDBKeys(db)
	log.Println("Len()", keys.Len())
	reply(c, success, keys)
}

func cmdDbs(c *Client) {
	dbs := c.godis.Dbs
	list := ds.NewList()
	for i := 0; i < len(dbs); i++ {
		list.Put(ds.CreateObjectFromString(strconv.Itoa(int(dbs[i].Id)), 0))
		list.Put(ds.CreateObjectFromString(dbs[i].DbName, 0))
	}
	reply(c, success, list)
}

func cmdDel(c *Client) {
	if !c.ts.LockDB(c.CurDB) {
		reply(c, err_ts_lock_timeout.Error(), nil)
		return
	}
	args, err := getArgs(c)
	if err != nil {
		reply(c, err.Error(), nil)
		return
	}
	for _, arg := range args {
		c.ts.DeleteDBKey(c.CurDB, arg)
	}
	reply(c, success, nil)
}

func cmdSset(c *Client) {
	log.Println("进入cmdSset()")
	if !c.ts.LockDB(c.CurDB) {
		reply(c, err_ts_lock_timeout.Error(), nil)
		return
	}
	log.Println("数据库加锁成功")
	args, err := getArgs(c)
	if err != nil {
		reply(c, err.Error(), nil)
		return
	}
	if len(args)%2 != 0 {
		reply(c, err_cmd.Error(), nil)
		return
	}
	for i := 0; i < len(args); i++ {
		c.ts.SetDBKey(c.CurDB, ds.STRING, args[i], args[i+1])
		i++
	}
	reply(c, success, nil)
}

func cmdSget(c *Client) {
	if !c.ts.RlockDB(c.CurDB) {
		reply(c, err_ts_lock_timeout.Error(), nil)
		return
	}
	args, err := getArgs(c)
	if err != nil {
		reply(c, err.Error(), nil)
		return
	}
	list := ds.NewList()
	for _, key := range args {
		obj := c.ts.GetDBKey(c.CurDB, key)
		if obj != nil {
			list.Put(obj)
		}
	}
	reply(c, success, list)
}

func cmdRollBack(c *Client) {
	if c.AutoCommit == true {
		reply(c, err_no_start_ts.Error(), nil)
		return
	}
	err := c.ts.RollBack(-1)
	if err != nil {
		reply(c, err_rollback_fail.Error(), nil)
		return
	}
	reply(c, success, nil)
}

func cmdCommit(c *Client) {
	if c.AutoCommit == true {
		reply(c, err_no_start_ts.Error(), nil)
		return
	}
	err := c.ts.Commit()
	c.AutoCommit = true
	if err != nil {
		reply(c, err_commit_back.Error(), nil)
		return
	}
	reply(c, success, nil)
}

func cmdRollBackTo(c *Client) {
	if c.AutoCommit == true {
		reply(c, err_no_start_ts.Error(), nil)
		return
	}
	args, err := getArgs(c)
	if err != nil {
		reply(c, err.Error(), nil)
		return
	}
	savepoint, err := strconv.Atoi(string(args[0]))
	log.Println("进入rollbackto")
	if err != nil {
		reply(c, err_not_found_savepoint.Error(), nil)
		return
	}
	err = c.ts.RollBack(savepoint)
	if err != nil {
		reply(c, err_rollback_fail.Error(), nil)
		return
	}
	reply(c, success, nil)
}

func cmdSavePoint(c *Client) {
	if c.AutoCommit == true {
		reply(c, err_no_start_ts.Error(), nil)
		return
	}
	c.ts.AddSavePoint()
	reply(c, success, nil)
}
