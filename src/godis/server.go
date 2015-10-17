package godis

import (
	"container/list"
	"github.com/Terry-Mao/goconf"
	"log"
	"net"
	"sync"
	"time"
)

type Godis struct {
	// 服务主机
	Host string
	// 服务端口
	Port string
	// 数据库
	Dbs []DB
	// 客户端连接
	Clients    *list.List
	ClientsMap map[*Client]*list.Element
	sync.Mutex
	// 最大客户端限制
	MaxClientsN int
	// 当前最大客户端数
	CurrentClientsN int
	// 系统日志输出位置
	SysLogPath string
	// 命令行参数数目限制
	Cmdargsnum int
	// 命令行一个参数长度限制(1M)
	Cmdargsize uint64
	// 事务锁超时时间
	Tstimeout time.Duration
}

func InitGodis() *Godis {
	return &Godis{
		Host:       "127.0.0.1",
		Port:       "1899",
		Dbs:        make([]DB, 64),
		Clients:    list.New(),
		ClientsMap: make(map[*Client]*list.Element),
	}
}

func InitServer(godis *Godis, c *goconf.Config) {
	ser := c.Get("server")
	if v, err := ser.String("host"); err == nil {
		godis.Host = v
	}
	if v, err := ser.String("port"); err == nil {
		godis.Port = v
	}
	if v, err := ser.Int("max_dbs"); err == nil {
		godis.Dbs = make([]DB, v)
	}
	for i := 0; i < len(godis.Dbs); i++ {
		InitDB(i, &godis.Dbs[i])
	}
	if v, err := ser.Int("max_client"); err == nil {
		godis.MaxClientsN = int(v)
	}
	if v, err := ser.Int("cmd_args_num"); err == nil {
		godis.Cmdargsnum = int(v)
	}
	if v, err := ser.Int("cmd_arg_size"); err == nil {
		godis.Cmdargsize = uint64(v)
	}
	if v, err := ser.Int("ts_trylock_timeout"); err == nil {
		godis.Tstimeout = time.Millisecond * time.Duration(v)
	}
}

func StartServer(godis *Godis) {
	log.Println("服务在", net.JoinHostPort(godis.Host, godis.Port))
	listen, err := net.Listen("tcp", net.JoinHostPort(godis.Host, godis.Port))
	if err != nil {
		log.Fatalln(err)
	}
	for {
		conn, err := listen.Accept()
		log.Println("接收到一个连接")
		if err != nil {
			log.Fatalln(err)
		}
		if godis.CurrentClientsN >= godis.MaxClientsN {
			log.Println("连接数达到上限")
			conn.Write([]byte("[error] err_client_max"))
			conn.Close()
			continue
		}
		log.Println("派发一个处理线程")
		go Process(NewClient(conn, godis))
	}
}
