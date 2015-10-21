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
	cmd_savepoint
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
	fmt.Println("begin")
	conn.Write(warp(cmd_begin, nil))
	data := readData(conn)
	fmt.Println(data)
	fmt.Println(string(data))

	fmt.Println("sset A a B b")
	args := make([]string, 4)
	args[0] = "A"
	args[1] = "a"
	args[2] = "B"
	args[3] = "b"
	conn.Write(warp(cmd_sset, args))
	data = readData(conn)
	fmt.Println(data)
	fmt.Println(string(data))

	fmt.Println("sset C c D d")
	args = make([]string, 4)
	args[0] = "C"
	args[1] = "c"
	args[2] = "D"
	args[3] = "d"
	conn.Write(warp(cmd_sset, args))
	data = readData(conn)
	fmt.Println(data)
	fmt.Println(string(data))

	fmt.Println("savepoint")
	conn.Write(warp(cmd_savepoint, nil))
	data = readData(conn)
	fmt.Println(data)
	fmt.Println(string(data))

	fmt.Println("sset XX x WW w QQ q")
	args = make([]string, 6)
	args[0] = "XX"
	args[1] = "x"
	args[2] = "WW"
	args[3] = "w"
	args[4] = "QQ"
	args[5] = "q"
	conn.Write(warp(cmd_sset, args))
	data = readData(conn)
	fmt.Println(data)
	fmt.Println(string(data))

	fmt.Println("savepoint")
	conn.Write(warp(cmd_savepoint, nil))
	data = readData(conn)
	fmt.Println(data)
	fmt.Println(string(data))

	fmt.Println("sset YY y RR r II i")
	args = make([]string, 6)
	args[0] = "YY"
	args[1] = "y"
	args[2] = "RR"
	args[3] = "r"
	args[4] = "II"
	args[5] = "i"
	conn.Write(warp(cmd_sset, args))
	data = readData(conn)
	fmt.Println(data)
	fmt.Println(string(data))

	fmt.Println("rollbackto 1")
	args = make([]string, 1)
	args[0] = "1"
	conn.Write(warp(cmd_rollbackto, args))
	data = readData(conn)
	fmt.Println(data)
	fmt.Println(string(data))

	fmt.Println("time.Sleep(time.Second * 10)")
	time.Sleep(time.Second * 10)

	fmt.Println("commit")
	conn.Write(warp(cmd_commit, args))
	data = readData(conn)
	fmt.Println(data)
	fmt.Println(string(data))
}
