package store

import (
	"errors"
	"fmt"
	"github.com/Terry-Mao/goconf"
	"log"
	"os"
)

const (
	TsLogMetaFilePrefix         = "tslogmeta_%d"
	TsLogFilePrefix             = "tslog_%d"
	DefaultTsLogFileMaxSize     = 1000000000
	DefaultTsLogMetaFileMaxSize = 1000000000
)

var (
	Err_ts_log_dir_not_dir = errors.New("'ts_data_dir' not is dir!")
)

type TsLog struct {
	dir      string
	metaFile *os.File
	dataFile *os.File

	dataFileMaxSize int
	metaFileMaxSize int

	dataFileMinN int
	dataFileMaxN int

	metaFileMinN int
	metaFileMaxN int
}

func NewTsLog(sec *goconf.Section) (*TsLog, error) {
	var (
		dir         string
		dataFileMax int = DefaultTsLogFileMaxSize
		metaFileMax int = DefaultTsLogMetaFileMaxSize
	)
	if v, err := sec.String("ts_data_dir"); err == nil {
		dir = v
	} else {
		if v, err := sec.String("datadir"); err == nil {
			dir = fmt.Sprintf("%s/%s", v, "ts")
		} else {
			log.Fatalln("Please configure dataDir!")
		}
	}
	if v, err := sec.Int("ts_log_max_size"); err == nil {
		dataFileMax = int(v)
	}
	if v, err := sec.Int("ts_metalog_max_size"); err == nil {
		metaFileMax = int(v)
	}
	fileinfo, err := os.Stat(dir)
	if err != nil {
		return nil, err
	}
	if !fileinfo.IsDir() {
		return nil, Err_ts_log_dir_not_dir
	}
	return &TsLog{
		dir:             dir,
		dataFileMaxSize: dataFileMax,
		metaFileMaxSize: metaFileMax,
	}, nil
}

func (dl *TsLog) scanTsMetaLogFile() error {
	return nil
}
