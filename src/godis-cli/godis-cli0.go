package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	// "time"
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
	data := readData(conn)
	fmt.Println(data)
	fmt.Println(string(data))

	fmt.Println("sset G g H h I i")
	args := make([]string, 6)
	args[0] = "G"
	args[1] = "g"
	args[2] = "H"
	args[3] = "h"
	args[4] = "I"
	args[5] = "i"
	conn.Write(warp(cmd_sset, args))
	data = readData(conn)
	fmt.Println(data)
	fmt.Println(string(data))

	fmt.Println("del I H")
	args = make([]string, 2)
	args[0] = "I"
	args[1] = "H"
	conn.Write(warp(cmd_del, args))
	data = readData(conn)
	fmt.Println(data)
	fmt.Println(string(data))

	// fmt.Println("commit")
	// conn.Write(warp(cmd_commit, args))
	// data = readData(conn)
	// fmt.Println(data)
	// fmt.Println(string(data))
}
