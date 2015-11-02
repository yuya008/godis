package tests

import (
	"bytes"
	"encoding/binary"
	"log"
	"net"
	"testing"
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
	cmd_savepoint
	cmd_rollbackto
	cmd_commit
)

type Req struct {
	conn net.Conn
}

func connectGodis(addr string) *Req {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Panicln(err)
	}
	return &Req{
		conn: conn,
	}
}

func (req *Req) request(cmd uint8, args ...string) {
	buffer := bytes.NewBuffer(nil)
	err := binary.Write(buffer, binary.BigEndian, &cmd)
	if err != nil {
		log.Panicln(err)
	}
	if len(args) == 0 {
		req.conn.Write(buffer.Bytes())
		return
	}
	var paraN uint16 = uint16(len(args))
	if len(args) == 0 {
		req.conn.Write(buffer.Bytes())
		return
	}
	err = binary.Write(buffer, binary.BigEndian, &paraN)
	if err != nil {
		log.Panicln(err)
	}
	for i := range args {
		var p1 uint64 = uint64(len(args[i]))
		err = binary.Write(buffer, binary.BigEndian, &p1)
		if err != nil {
			log.Fatalln(err)
		}
		buffer.WriteString(args[i])
	}
	req.conn.Write(buffer.Bytes())
}

func (req *Req) response() (int32, string) {
	var (
		resultN int32
	)
	binary.Read(req.conn, binary.BigEndian, &resultN)
}

func TestSsetAndSget(t *testing.T) {
	conn := connectGodis("127.0.0.1:1899")
	conn.request(cmd_sset, "", "", "")
}
