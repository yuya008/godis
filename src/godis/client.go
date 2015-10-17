package godis

import (
	"bufio"
	"log"
	"net"
)

const (
	CLIENT_WAIT    = 0
	CLIENT_PARSING = 1
	CLIENT_PROCESS = 2
	CLIENT_CLOSE   = 3
)

type Client struct {
	net.Conn
	godis      *Godis
	CurDB      *DB
	AutoCommit bool
	CmdError   bool
	R          *bufio.Reader
	W          *bufio.Writer
	ts         *Ts
}

func NewClient(c net.Conn, godis *Godis) *Client {
	cli := new(Client)
	cli.CurDB = &godis.Dbs[0]
	cli.godis = godis
	cli.AutoCommit = true
	cli.Conn = c
	cli.R = bufio.NewReader(c)
	cli.W = bufio.NewWriter(c)
	cli.CmdError = false
	log.Println("创建一个客户端", cli)
	return cli
}

func (c *Client) Cancel() {
	c.Conn.Close()
	log.Println("注销一个客户端")
}

func (c *Client) ReplyBytes(data []byte) (int, error) {
	return c.Conn.Write(data)
}

func (c *Client) ReplyString(data string) (int, error) {
	return c.Conn.Write([]byte(data))
}
