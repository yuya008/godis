package server

import (
	"encoding/binary"
	"errors"
	_ "log"
	"object"
	"strings"
)

func Process(client *Client) {
	for {
		cmd := parseCmd(client)
		switch cmd {
		case "set":
			cmdSet(client)
		case "get":
			cmdGet(client)
		default:
			break
		}
	}
	client.Cancel()
}

func parseCmd(client *Client) string {
	var cmdlen int16
	err := binary.Read(client.R, binary.BigEndian, &cmdlen)
	if err != nil || cmdlen == 0 {
		client.Conn.Write([]byte("[error] err_cmd"))
		return ""
	}
	cmd := make([]byte, cmdlen)
	n, err := client.R.Read(cmd)
	if err != nil {
		client.Conn.Write([]byte("[error] err_read_cmd"))
		return ""
	}
	return strings.ToLower(string(cmd[:n]))
}

func readKey(client *Client) (string, error) {
	var keylen int16
	err := binary.Read(client.R, binary.BigEndian, &keylen)
	if err != nil || keylen == 0 {
		return "", errors.New("err_cmd")
	}
	key := make([]byte, keylen)
	n, err := client.R.Read(key)
	if err != nil {
		return "", errors.New("err_read_cmd")
	}
	return string(key[:n]), nil
}

func readValue(client *Client) ([]byte, error) {
	var valuelen uint64
	err := binary.Read(client.R, binary.BigEndian, &valuelen)
	if err != nil || valuelen == 0 {
		return nil, errors.New("err_cmd")
	}
	value := make([]byte, valuelen)
	n, err := client.R.Read(value)
	if err != nil {
		return nil, errors.New("err_read_cmd")
	}
	return value[:n], nil
}

func cmdSet(c *Client) {
	key0, err := readKey(c)
	if err != nil {
		c.Conn.Write([]byte(err.Error()))
		return
	}
	value0, err := readValue(c)
	if err != nil {
		c.Conn.Write([]byte(err.Error()))
		return
	}
	valObj := object.CreateStringObject(value0)
	curdb := c.CurDB
	curdb.Lock()
	defer curdb.Unlock()
	curdb.Data[key0] = valObj
}

func cmdGet(c *Client) {

}
