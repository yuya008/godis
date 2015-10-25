package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	_ "time"
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
	cmd_rollbackto
	cmd_commit
)

func warp(cmd uint8, sql []string) []byte {
	buffer := bytes.NewBuffer(nil)
	err := binary.Write(buffer, binary.BigEndian, &cmd)
	if err != nil {
		log.Fatalln(err)
	}
	if sql == nil {
		return buffer.Bytes()
	}
	var paraN uint16 = uint16(len(sql))
	if len(sql) == 0 {
		return buffer.Bytes()
	}
	err = binary.Write(buffer, binary.BigEndian, &paraN)
	if err != nil {
		log.Fatalln(err)
	}
	for i := range sql {
		var p1 uint64 = uint64(len(sql[i]))
		err = binary.Write(buffer, binary.BigEndian, &p1)
		if err != nil {
			log.Fatalln(err)
		}
		buffer.Write([]byte(sql[i]))
	}
	return buffer.Bytes()
}

func readData(conn net.Conn) []byte {
	buffer := make([]byte, 2048)
	n, err := conn.Read(buffer)
	if err != nil {
		log.Fatalln(err)
	}
	return buffer[:n]
}

func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:1899")
	if err != nil {
		log.Fatalln(err)
	}
	args := make([]string, 4)
	args[0] = "A"
	args[1] = "a"
	args[2] = "B"
	args[3] = "b"
	conn.Write(warp(cmd_sset, args))
	result2 := readData(conn)
	fmt.Println(result2)
	fmt.Println(string(result2))

	args = make([]string, 4)
	args[0] = "C"
	args[1] = "c"
	args[2] = "D"
	args[3] = "d"
	conn.Write(warp(cmd_sset, args))
	result2 = readData(conn)
	fmt.Println(result2)
	fmt.Println(string(result2))
}
