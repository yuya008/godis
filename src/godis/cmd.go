package godis

import (
	golist "container/list"
	ds "data_struct"
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

func getCurrentDb(ts *Ts, db *DB) (*DB, error) {
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
	list := golist.New()
	for i := 0; i < len(dbs); i++ {
		list.PushBack(ds.CreateObjectFromString(strconv.Itoa(dbs[i].Id), 0))
		list.PushBack(ds.CreateObjectFromString(dbs[i].DbName, 0))
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
		c.ts.DeleteDBKey(c.CurDB, string(arg))
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
		c.ts.SetDBKey(c.CurDB, string(args[i]), args[i+1])
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
	list := golist.New()
	for _, key := range args {
		obj := c.ts.GetDBKey(c.CurDB, string(key))
		if obj != nil {
			list.PushBack(obj)
		}
	}
	reply(c, success, list)
}

func cmdRollBack(c *Client) {
	if c.AutoCommit == true {
		reply(c, err_no_start_ts.Error(), nil)
		return
	}
	c.ts.RollBack()
	reply(c, success, nil)
}

func cmdCommit(c *Client) {
	if c.AutoCommit == true {
		reply(c, err_no_start_ts.Error(), nil)
		return
	}
	c.ts.Commit()
	c.AutoCommit = true
	reply(c, success, nil)
}
