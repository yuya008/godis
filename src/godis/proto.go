package godis

import (
	golist "container/list"
	ds "data_struct"
	"encoding/binary"
	"errors"
	"log"
)

const (
	cmd_keys = iota
	cmd_use
	cmd_dbs
	cmd_bye
	cmd_del
	cmd_sset
	cmd_sget
	cmd_put
	cmd_pop
	cmd_begin
	cmd_rollback
	cmd_commit
)

var (
	success               = "[ok] success!"
	err_cmd               = errors.New("[error] cmd is Illegal!")
	err_cmd_not_found     = errors.New("[error] cmd not found!")
	err_cmd_read          = errors.New("[error] cmd read fail!")
	err_db_not_found      = errors.New("[error] db not found!")
	err_cmd_args_too_long = errors.New("[error] cmd args too long!")
	err_cmd_arg_too_large = errors.New("[error] cmd args too large!")
	err_no_start_ts       = errors.New("[error] no open transaction!")
)

func reply(c *Client, msg string, result *golist.List) {
	defer c.W.Flush()
	var resultN int32
	if result == nil {
		resultN = -1
	}
	if msg == success {
		resultN = 0
		if result != nil {
			resultN = int32(result.Len())
		}
	} else {
		c.CmdError = true
	}
	err := binary.Write(c.W, binary.BigEndian, &resultN)
	if err != nil {
		log.Println("reply()", err)
		return
	}
	if len(msg) == 0 {
		log.Println("reply()", "msg len == 0")
		return
	}
	var msgLen uint8 = uint8(len(msg))
	err = binary.Write(c.W, binary.BigEndian, &msgLen)
	if err != nil {
		log.Println("reply()", err)
		return
	}
	c.W.WriteString(msg)
	if result == nil || resultN == 0 {
		return
	}
	var objlen uint64
	var obj *ds.Object
	var ok bool
	for e := result.Front(); e != nil; e = e.Next() {
		if obj, ok = e.Value.(*ds.Object); !ok {
			continue
		}
		objlen = uint64(len(obj.GetBuffer()))
		if objlen == 0 {
			continue
		}
		err = binary.Write(c.W, binary.BigEndian, &objlen)
		if err != nil {
			log.Println("reply()", err)
			return
		}
		_, err = c.W.Write(obj.GetBuffer())
		if err != nil {
			log.Println("reply()", err)
			return
		}
	}
}

func getArgs(c *Client) ([][]byte, error) {
	var args uint16
	err := binary.Read(c.R, binary.BigEndian, &args)
	if err != nil || args == 0 {
		if args == 0 {
			return nil, err_cmd
		}
		return nil, err_cmd_read
	}
	log.Println("参数数量", args)
	if uint16(c.godis.Cmdargsnum) < args {
		return nil, err_cmd_args_too_long
	}
	var i uint16
	var arglen uint64
	var nowarglen uint64
	retval := make([][]byte, args)
	for ; i < args; i++ {
		err = binary.Read(c.R, binary.BigEndian, &arglen)
		if err != nil {
			return nil, err_cmd_read
		}
		nowarglen += arglen
		if nowarglen > c.godis.Cmdargsize {
			return nil, err_cmd_arg_too_large
		}
		arg := make([]byte, arglen)
		n, err := c.R.Read(arg)
		if err != nil {
			return nil, err_cmd_read
		}
		retval[i] = arg[:n]
	}
	return retval, nil
}

func parseCmd(c *Client) (uint8, error) {
	var cmd uint8
	err := binary.Read(c.R, binary.BigEndian, &cmd)
	if err != nil || cmd > cmd_commit {
		return 0, err_cmd_not_found
	}
	return cmd, nil
}

func Process(c *Client) {
	defer func() {
		c.ts.RollBack()
		c.Cancel()
	}()
	for {
		if c.AutoCommit {
			log.Println("创建事务")
			c.ts = NewTs(c.godis)
		}
		cmd, err := parseCmd(c)
		log.Println("分析命令行", cmd, err)
		if err != nil {
			return
		}
		switch cmd {
		case cmd_use:
			cmdUse(c)
		case cmd_keys:
			cmdKeys(c)
		case cmd_begin:
			cmdBegin(c)
		case cmd_dbs:
			cmdDbs(c)
		case cmd_del:
			cmdDel(c)
		case cmd_sset:
			cmdSset(c)
		case cmd_sget:
			cmdSget(c)
		case cmd_rollback:
			cmdRollBack(c)
			continue
		case cmd_commit:
			log.Println("手动提交事务")
			cmdCommit(c)
			continue
		}
		if c.CmdError {
			return
		}
		if c.AutoCommit {
			log.Println(c.ts.TsId, "事务自动提交")
			c.ts.Commit()
		}
	}
}
