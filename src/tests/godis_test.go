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

type Result struct {
	n   int32
	msg string
	res []string
}

func newResult(n int32, msg string, res []string) *Result {
	return &Result{
		n:   n,
		msg: msg,
		res: res,
	}
}

func (res *Result) ResultN() int32 {
	return res.n
}

func (res *Result) Success() bool {
	return res.n >= 0
}

func (res *Result) Message() string {
	return res.msg
}

func (res *Result) GetResult() []string {
	return res.res
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
	if len(args) == 0 || args[0] == "" {
		req.conn.Write(buffer.Bytes())
		return
	}
	var paraN uint16 = uint16(len(args))
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

func (req *Req) response() *Result {
	var (
		resultN int32
		i       int32
		msgLen  uint8
		result  []string
		objlen  uint64
	)
	err := binary.Read(req.conn, binary.BigEndian, &resultN)
	if err != nil {
		log.Panicln(err)
	}
	err = binary.Read(req.conn, binary.BigEndian, &msgLen)
	if err != nil {
		log.Panicln(err)
	}
	msgbuf := make([]byte, msgLen)
	_, err = req.conn.Read(msgbuf)
	if err != nil {
		log.Panicln(err)
	}
	if resultN <= 0 {
		return newResult(resultN, string(msgbuf), nil)
	}
	result = make([]string, resultN)
	for ; i < resultN; i++ {
		err = binary.Read(req.conn, binary.BigEndian, &objlen)
		if err != nil || objlen == 0 {
			log.Panicln(err)
		}
		objbuf := make([]byte, objlen)
		_, err = req.conn.Read(objbuf)
		if err != nil {
			log.Panicln(err)
		}
		result[i] = string(objbuf)
	}
	return newResult(resultN, string(msgbuf), result)
}

func (req *Req) destory() {
	req.conn.Close()
}

func (req *Req) Call(cmd uint8, args ...string) *Result {
	req.request(cmd, args...)
	return req.response()
}

func TestSsetAndSget0(t *testing.T) {
	req := connectGodis("127.0.0.1:1899")
	res := req.Call(cmd_sset, "foo", "你好", "bar", "Hello")
	if res.Success() {
		res = req.Call(cmd_sget, "foo", "bar")
		if res.Success() {
			r := res.GetResult()
			if len(r) < 2 {
				t.Error("sget result error!", res.Message())
			}
			if r[0] != "你好" || r[1] != "Hello" {
				t.Error("sget result error!", res.Message())
			}
		} else {
			t.Error("sget error!", res.Message())
		}
	} else {
		t.Error("sset error!", res.Message())
	}
	req.destory()
}

func TestSsetAndSget1(t *testing.T) {
	req := connectGodis("127.0.0.1:1899")
	res := req.Call(cmd_begin, "")
	if !res.Success() {
		t.Error("事务开始失败!", res.Message())
	}
	res = req.Call(cmd_sset, "foo", "golang", "bar",
		"godis", "godis", "万岁")
	if !res.Success() {
		t.Error("存入数据失败!")
	}
	res = req.Call(cmd_sget, "foo", "godis")
	if !res.Success() {
		t.Error("取得数据失败!", res.Message())
	} else {
		r := res.GetResult()
		if len(r) < 2 {
			t.Error("取得数据错误!", res.ResultN(), res.Message())
		}
		if r[0] != "golang" || r[1] != "万岁" {
			t.Error("取得数据错误!", res.Message())
		}
	}
	res = req.Call(cmd_commit, "")
	if !res.Success() {
		t.Error("提交事务失败")
	}
	res = req.Call(cmd_sget, "bar", "foo")
	if !res.Success() {
		t.Error("取得数据失败!", res.Message())
	} else {
		r := res.GetResult()
		if len(r) < 2 {
			t.Error("取得数据错误!", res.Message())
		}
		if r[0] != "godis" || r[1] != "golang" {
			t.Error("取得数据错误!", res.Message())
		}
	}
	req.destory()
}

func TestSsetAndSget2(t *testing.T) {
	req := connectGodis("127.0.0.1:1899")
	res := req.Call(cmd_begin, "")
	if !res.Success() {
		t.Error("事务开始失败!", res.Message())
	}
	res = req.Call(cmd_sset, "foo", "C++", "内存", "磁盘",
		"CPU", "SSD")
	if !res.Success() {
		t.Error("存入数据失败!")
	}
	res = req.Call(cmd_sget, "foo", "CPU")
	if !res.Success() {
		t.Error("取得数据失败!", res.Message())
	}
	r := res.GetResult()
	if len(r) < 2 {
		t.Error("取得数据错误!", res.Message())
	}
	if r[0] != "C++" || r[1] != "SSD" {
		t.Error("取得数据错误!", res.Message())
	}
	res = req.Call(cmd_rollback, "")
	if !res.Success() {
		t.Error("回滚出错!", res.Message())
	}
	res = req.Call(cmd_sget, "foo", "CPU", "内存")
	if res.Success() {
		r := res.GetResult()
		if len(r) != 1 {
			t.Error("取得数据错误!", res.Message())
		}
		if r[0] != "golang" {
			t.Error("取得数据错误!", res.Message())
		}
	} else {
		t.Error("取得数据错误!", res.Message())
	}
	req.destory()
}

func TestSsetAndSget3(t *testing.T) {
	req := connectGodis("127.0.0.1:1899")
	res := req.Call(cmd_begin, "")
	if !res.Success() {
		t.Error("事务开始失败!", res.Message())
	}
	res = req.Call(cmd_sset,
		"php",
		"超文本预处理器",
		"Ruby",
		"一种为简单快捷的面向对象编程（面向对象程序设计）而创的脚本语言",
		"Perl",
		"一种功能丰富的计算机程序语言",
	)
	if !res.Success() {
		t.Error("存入数据失败!", res.Message())
	}
	// 0
	res = req.Call(cmd_savepoint, "")
	if !res.Success() {
		t.Error("设置回滚点失败!", res.Message())
	}
	res = req.Call(cmd_sset,
		"C++",
		"C++是在C语言的基础上开发的一种面向对象编程语言",
		"Erlang",
		"Erlang是一种通用的面向并发的编程语言",
		"Rust",
		"Rust是Mozilla开发的注重安全、性能和并发性的编程语言",
	)
	if !res.Success() {
		t.Error("存入数据失败!", res.Message())
	}
	// 回滚0
	res = req.Call(cmd_rollbackto, "0")
	if !res.Success() {
		t.Error("设置回滚点失败!", res.Message())
	}
	// 1
	res = req.Call(cmd_savepoint, "")
	if !res.Success() {
		t.Error("设置回滚点失败!", res.Message())
	}
	res = req.Call(cmd_sset,
		"Java",
		"Java是一种可以撰写跨平台应用程序的面向对象的程序设计语言",
		"C",
		"C语言是一门通用计算机编程语言，应用广泛",
		"Pascal",
		"Pascal语言语法严谨，层次分明，程序易写，可读性强，是第一个结构化编程语言",
	)
	if !res.Success() {
		t.Error("存入数据失败!", res.Message())
	}
	// 2
	res = req.Call(cmd_savepoint, "")
	if !res.Success() {
		t.Error("设置回滚点失败!", res.Message())
	}
	res = req.Call(cmd_sset,
		"Fortran",
		"它是世界上最早出现的计算机高级程序设计语言，广泛应用于科学和工程计算领域",
		"Python",
		"是一种面向对象、解释型计算机程序设计语言",
		"C#",
		"C#是微软公司发布的一种面向对象的、运行于.NET Framework之上的高级程序设计语言",
	)
	if !res.Success() {
		t.Error("存入数据失败!", res.Message())
	}
	// 回滚
	res = req.Call(cmd_rollbackto, "2")
	if !res.Success() {
		t.Error("设置回滚点失败!", res.Message())
	}
	res = req.Call(cmd_commit, "")
	if !res.Success() {
		t.Error("提交事务失败")
	}
	// get
	res = req.Call(cmd_sget, "Java", "C#", "Rust", "php")
	if !res.Success() {
		t.Error("取得数据失败!", res.Message())
	}
	r := res.GetResult()
	if len(r) != 2 {
		t.Error("取得数据错误!", res.Message())
	}
	if len(r[0]) < 10 && len(r[1]) < 10 {
		t.Error("取得数据错误!", res.GetResult())
	}
	req.destory()
}

func TestKeys0(t *testing.T) {
	should := map[string]bool{
		"foo":    true,
		"bar":    true,
		"godis":  true,
		"php":    true,
		"Ruby":   true,
		"Perl":   true,
		"Pascal": true,
		"Java":   true,
		"C":      true,
	}

	req := connectGodis("127.0.0.1:1899")
	res := req.Call(cmd_keys, "")
	if res.Success() {
		r := res.GetResult()
		// t.Log(r)
		if len(r) != len(should) {
			t.Error("和应有的结果长度不符")
		}
		for _, k := range r {
			_, ok := should[k]
			if !ok {
				t.Error("和应有的结果不符")
				break
			}
		}
	} else {
		t.Error("keys 调用失败!")
	}
	req.destory()
}
