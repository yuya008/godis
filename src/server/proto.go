package server

import (
	"bytes"
	"encoding/binary"
	"errors"
	"log"
	"object"
	"strconv"
	"strings"
)

var (
	success             = "[ok] success!"
	BYE                 = "[ok] bye!"
	err_read_cmd        = errors.New("[error] read command fail!")
	err_cmd             = errors.New("[error] command illegal!")
	err_key_not_found   = errors.New("[error] the key not found!")
	err_value_too_large = errors.New("[error] the value too large!")
	err_db_not_found    = errors.New("[error] the db not found!")
)

func Process(client *Client) {
	defer client.Cancel()
	for {
		log.Println("等待数据并分析")
		cmd, err := parseCmd(client)
		log.Println("分析数据得到", cmd, err)
		switch cmd {
		case "set":
			cmdSet(client)
		case "get":
			cmdGet(client)
		case "dbs":
			cmdDbs(client)
		case "use":
			cmdUse(client)
		case "keys":
			cmdKeys(client)
		case "bye":
			cmdBye(client)
			log.Println("客户端退出")
			return
		default:
			reply(client, err_cmd.Error(), nil)
			return
		}
	}
}

func parseCmd(client *Client) (string, error) {
	var cmdlen int16
	err := binary.Read(client.R, binary.BigEndian, &cmdlen)
	if err != nil || cmdlen == 0 {
		if err != nil {
			return "", err_read_cmd
		} else {
			return "", err_cmd
		}
	}
	cmd := make([]byte, cmdlen)
	n, err := client.R.Read(cmd)
	if err != nil {
		return "", err_read_cmd
	}
	return strings.ToLower(string(cmd[:n])), nil
}

func readKey(client *Client) (string, error) {
	var keylen int16
	err := binary.Read(client.R, binary.BigEndian, &keylen)
	if err != nil || keylen == 0 {
		return "", err_cmd
	}
	key := make([]byte, keylen)
	n, err := client.R.Read(key)
	if err != nil {
		return "", err_read_cmd
	}
	return string(key[:n]), nil
}

func readValue(client *Client) ([]byte, error) {
	var valuelen uint64
	err := binary.Read(client.R, binary.BigEndian, &valuelen)
	if err != nil || valuelen == 0 {
		return nil, err_cmd
	}
	value := make([]byte, valuelen)
	n, err := client.R.Read(value)
	if err != nil {
		return nil, err_read_cmd
	}
	return value[:n], nil
}

func cmdSet(c *Client) {
	key0, err := readKey(c)
	if err != nil {
		reply(c, err.Error(), nil)
		return
	}
	value0, err := readValue(c)
	if err != nil {
		reply(c, err.Error(), nil)
		return
	}
	valObj := object.CreateStringObject(value0)
	curdb := c.CurDB
	curdb.Lock()
	curdb.Data[key0] = valObj
	curdb.Unlock()
	reply(c, success, nil)
}

func cmdGet(c *Client) {
	key0, err := readKey(c)
	if err != nil {
		reply(c, err.Error(), nil)
		return
	}
	curdb := c.CurDB
	curdb.Lock()
	valObj, ok := curdb.Data[key0]
	curdb.Unlock()
	if !ok {
		reply(c, err_key_not_found.Error(), nil)
		return
	}
	reply(c, success, valObj.GetRealData())
}
func cmdDbs(c *Client) {
	dbs := c.godis.Dbs
	buffer := bytes.NewBuffer(nil)
	for _, db := range dbs {
		buffer.WriteString(strconv.Itoa(db.Id))
		buffer.WriteByte(' ')
		buffer.WriteString(db.DbName)
		buffer.WriteByte('\n')
	}
	reply(c, success, buffer.Bytes())
}
func cmdUse(c *Client) {
	witchdb, err := readKey(c)
	if err != nil {
		reply(c, err.Error(), nil)
		return
	}
	id, err := strconv.Atoi(witchdb)
	dbs := c.godis.Dbs
	if err != nil {
		for i, db := range dbs {
			if db.DbName == witchdb {
				c.CurDB = &dbs[i]
				goto end
			}
		}
	} else {
		for i, db := range dbs {
			if db.Id == id {
				c.CurDB = &dbs[i]
				goto end
			}
		}
	}
	reply(c, err_db_not_found.Error(), nil)
	return
end:
	reply(c, success, nil)
}

func cmdKeys(c *Client) {
	db := c.CurDB
	buffer := bytes.NewBuffer(nil)
	db.Lock()
	for key, _ := range db.Data {
		buffer.WriteString(key)
		buffer.WriteByte('\n')
	}
	db.Unlock()
	reply(c, success, buffer.Bytes())
}

func cmdBye(c *Client) {
	reply(c, BYE, nil)
}

func reply(c *Client, status string, data []byte) {
	var statuslen int16 = int16(len(status) + 1)
	buffer := bytes.NewBuffer(nil)
	err := binary.Write(buffer, binary.BigEndian, &statuslen)
	if err != nil {
		return
	}
	buffer.WriteString(status)
	buffer.WriteByte('\n')
	if data != nil {
		var datalen uint64 = uint64(len(data))
		err := binary.Write(buffer, binary.BigEndian, &datalen)
		if err != nil {
			return
		}
		buffer.Write(data)
	}
	c.ReplyBytes(buffer.Bytes())
}
