package server

import (
	"bufio"
	"db"
	"net"
)

const (
	CLIENT_WAIT    = 0
	CLIENT_PARSING = 1
	CLIENT_PROCESS = 2
)

type Client struct {
	godis *Godis
	net.Conn
	R      *bufio.Reader
	W      *bufio.Writer
	CurDB  *db.DB
	Status uint8
	Txing  bool
}

func NewClient(c net.Conn, godis *Godis) *Client {
	cli := new(Client)
	cli.Conn = c
	cli.CurDB = &godis.Dbs[0]
	cli.godis = godis
	cli.Status = CLIENT_WAIT
	cli.R = bufio.NewReader(cli.Conn)
	cli.W = bufio.NewWriter(cli.Conn)
	godis.Lock()
	defer godis.Unlock()

	godis.ClientsMap[cli] = godis.Clients.PushBack(cli)
	godis.CurrentClientsN++

	return cli
}

func (c *Client) Cancel() {
	c.Conn.Close()
	c.godis.Lock()
	defer c.godis.Unlock()

	c.godis.Clients.Remove(c.godis.ClientsMap[c])
	delete(c.godis.ClientsMap, c)
	c.godis.CurrentClientsN--
}
