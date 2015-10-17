package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"time"
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
	fmt.Println("begin")
	conn.Write(warp(cmd_begin, nil))
	result0 := readData(conn)
	fmt.Println(result0)
	fmt.Println(string(result0))

	fmt.Println("sget A C")
	args := make([]string, 2)
	args[0] = "A"
	args[1] = "C"
	conn.Write(warp(cmd_sget, args))
	result1 := readData(conn)
	fmt.Println(result1)
	fmt.Println(string(result1))

	time.Sleep(time.Second * 10)

	fmt.Println("keys")
	conn.Write(warp(cmd_keys, nil))
	result2 := readData(conn)
	fmt.Println(result2)
	fmt.Println(string(result2))
}
