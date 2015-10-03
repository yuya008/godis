package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"net"
)

func createKey(str string) ([]byte, error) {
	var keylen int16 = int16(len(str))

	buf := bytes.NewBuffer(nil)
	err := binary.Write(buf, binary.BigEndian, &keylen)
	if err != nil {
		return nil, err
	}
	buf.WriteString(str)
	return buf.Bytes(), nil
}

func createValue(str string) ([]byte, error) {
	var valuelen uint64 = uint64(len(str))

	buf := bytes.NewBuffer(nil)
	err := binary.Write(buf, binary.BigEndian, &valuelen)
	if err != nil {
		return nil, err
	}
	buf.WriteString(str)
	return buf.Bytes(), nil
}

func set(conn net.Conn, key, value string) {
	buffer := bytes.NewBuffer(nil)

	cmd, err := createKey("set")
	if err != nil {
		log.Fatalln(err)
	}
	buffer.Write(cmd)

	key0, err := createKey(key)
	if err != nil {
		log.Fatalln(err)
	}
	buffer.Write(key0)

	val, err := createValue(value)
	if err != nil {
		log.Fatalln(err)
	}
	buffer.Write(val)
	conn.Write(buffer.Bytes())
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println(string(buf[:n]))
}

func get(conn net.Conn, key string) {
	buffer := bytes.NewBuffer(nil)

	cmd, err := createKey("GET")
	if err != nil {
		log.Fatalln(err)
	}
	buffer.Write(cmd)

	key0, err := createKey(key)
	if err != nil {
		log.Fatalln(err)
	}
	buffer.Write(key0)
	conn.Write(buffer.Bytes())

	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println(string(buf[:n]))
}

func use(conn net.Conn, db string) {
	buffer := bytes.NewBuffer(nil)
	cmd, err := createKey("USe")
	if err != nil {
		log.Fatalln(err)
	}
	buffer.Write(cmd)

	dbname, err := createKey(db)
	if err != nil {
		log.Fatalln(err)
	}
	buffer.Write(dbname)
	conn.Write(buffer.Bytes())

	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println(string(buf[:n]))
}

func dbs(conn net.Conn) {
	buffer := bytes.NewBuffer(nil)
	cmd, err := createKey("DBS")
	if err != nil {
		log.Fatalln(err)
	}
	buffer.Write(cmd)
	conn.Write(buffer.Bytes())
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println(string(buf[:n]))
}

func keys(conn net.Conn) {
	buffer := bytes.NewBuffer(nil)
	cmd, err := createKey("KEYS")
	if err != nil {
		log.Fatalln(err)
	}
	buffer.Write(cmd)
	conn.Write(buffer.Bytes())
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println(string(buf[:n]))
}

func bye(conn net.Conn) {
	buffer := bytes.NewBuffer(nil)
	cmd, err := createKey("BYE")
	if err != nil {
		log.Fatalln(err)
	}
	buffer.Write(cmd)
	conn.Write(buffer.Bytes())
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println(string(buf[:n]))
}

func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:1899")
	if err != nil {
		log.Fatalln(err)
	}
	dbs(conn)
	set(conn, "好", "余亚0")
	set(conn, "1e", "余亚1")
	set(conn, "2r", "余亚2")
	set(conn, "3h", "余亚3")
	get(conn, "好")
	keys(conn)
	use(conn, "db2")
	keys(conn)
	bye(conn)
	conn.Close()
}
