package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/Terry-Mao/goconf"
	"log"
	"os"
	"server"
)

func usage(s error) {
	fmt.Println("godis usage:")
	fmt.Println(s)
	fmt.Println("./godis [config file 'godis.conf']")
	os.Exit(1)
}

func parseArgs() *goconf.Config {
	flag.Parse()
	args := flag.Args()
	if len(args) <= 0 {
		usage(errors.New("not found config file"))
	}
	conf := goconf.New()
	if err := conf.Parse(args[0]); err != nil {
		usage(err)
	}
	return conf
}

func main() {
	log.Println("初始化godis")
	godis_ptr := server.InitGodis()
	server.InitServer(godis_ptr, parseArgs())
	server.StartServer(godis_ptr)
}
