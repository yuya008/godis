package store

import (
	"bytes"
	ds "data_struct"
	"db"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/Terry-Mao/goconf"
	"log"
	"os"
	"path"
	"sync"
	"utils"
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
	mutex0       sync.Mutex
	mutex1       sync.Mutex
}

type RecordPosition struct {
	FileNo uint16
	Offset int64
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
	tslog := &TsLog{
		dir:             dir,
		dataFileMaxSize: dataFileMax,
		metaFileMaxSize: metaFileMax,
	}
	tslog.scanTsLogFile()
	return tslog, nil
}

func (dl *TsLog) Load() {

}

func (rp *RecordPosition) open() {

}

func (dl *TsLog) SetStatus(rp *RecordPosition, s uint8) {
	rp.open()
}

func (dl *TsLog) scanTsLogFile() error {
	dir, err := os.Open(dl.dir)
	if err != nil {
		log.Fatalln(err)
	}
	files, err := dir.Readdirnames(0)
	if err != nil {
		return err
	}
	dl.metaFileMaxN, dl.metaFileMinN = utils.FindFileNMaxAndMin(files, TsLogMetaFilePrefix)
	dl.metaFile, err = os.OpenFile(dl.GetMetaFilePath(dl.metaFileMaxN),
		os.O_RDWR|os.O_CREATE|os.O_APPEND, 0700)
	if err != nil {
		return err
	}

	dl.dataFileMaxN, dl.dataFileMinN = utils.FindFileNMaxAndMin(files, TsLogFilePrefix)
	dl.dataFile, err = os.OpenFile(dl.GetDataFilePath(dl.dataFileMaxN),
		os.O_RDWR|os.O_CREATE|os.O_APPEND, 0700)
	if err != nil {
		return err
	}
	return nil
}

func (dl *TsLog) GetMetaFilePath(no int) string {
	return path.Join(dl.dir, fmt.Sprintf(TsLogMetaFilePrefix, no))
}

func (dl *TsLog) GetDataFilePath(no int) string {
	return path.Join(dl.dir, fmt.Sprintf(TsLogFilePrefix, no))
}

func (dl *TsLog) PutAMeta(
	status uint8,
	id uint64,
	offsetlist ds.List,
) *RecordPosition {
	buffer := bytes.NewBuffer(nil)
	err := binary.Write(buffer, binary.BigEndian, &status)
	if err != nil {
		return nil
	}
	err = binary.Write(buffer, binary.BigEndian, &id)
	if err != nil {
		return nil
	}
	var tn uint16 = uint16(offsetlist.Len())
	err = binary.Write(buffer, binary.BigEndian, &tn)
	if err != nil {
		return nil
	}
	isswitchfile := dl.testFileSize(dl.metaFile, int64(dl.metaFileMaxSize))
	var wfilen uint16 = uint16(dl.metaFileMaxN)
	if isswitchfile {
		wfilen++
	}
	for e := offsetlist.GetFirstNode(); e != nil; e = e.Next {
		if v, ok := e.Value.(*RecordPosition); ok {
			err := binary.Write(buffer, binary.BigEndian, &v.FileNo)
			if err != nil {
				return nil
			}
			err = binary.Write(buffer, binary.BigEndian, &v.Offset)
			if err != nil {
				return nil
			}
		}
	}
	dl.mutex1.Lock()
	defer dl.mutex1.Unlock()
	if isswitchfile {
		dl.switchMetaFile()
	}
	dl.metaFile.Write(buffer.Bytes())
	offset, err := dl.metaFile.Seek(0, os.SEEK_CUR)
	if err != nil {
		return nil
	}
	return &RecordPosition{
		FileNo: uint16(dl.metaFileMaxSize),
		Offset: offset,
	}
}

func (dl *TsLog) Put(db *db.DB, objtype uint8, args ...[]byte) *RecordPosition {
	buffer := bytes.NewBuffer(nil)
	// db no
	err := binary.Write(buffer, binary.BigEndian, &db.Id)
	if err != nil {
		return nil
	}
	// type
	err = binary.Write(buffer, binary.BigEndian, &objtype)
	if err != nil {
		return nil
	}
	// argsN
	var argsN uint16 = uint16(len(args))
	err = binary.Write(buffer, binary.BigEndian, &argsN)
	if err != nil {
		return nil
	}
	var argLen uint64
	for _, d := range args {
		argLen = uint64(len(d))
		err = binary.Write(buffer, binary.BigEndian, &argLen)
		if err != nil {
			return nil
		}
		buffer.Write(d)
	}
	dl.mutex0.Lock()
	defer dl.mutex0.Unlock()
	if dl.testFileSize(dl.dataFile, int64(dl.dataFileMaxSize)) {
		dl.switchDataFile()
	}
	dl.dataFile.Write(buffer.Bytes())
	offset, err := dl.dataFile.Seek(0, os.SEEK_CUR)
	if err != nil {
		return nil
	}
	return &RecordPosition{
		FileNo: uint16(dl.dataFileMaxSize),
		Offset: offset,
	}
}

func (dl *TsLog) switchMetaFile() {
	var err error
	dl.metaFile.Close()
	dl.metaFileMaxN++
	dl.metaFile, err = os.OpenFile(dl.GetMetaFilePath(dl.metaFileMaxN),
		os.O_RDWR|os.O_CREATE|os.O_APPEND, 0700)
	if err != nil {
		log.Fatalln(err)
	}
}

func (dl *TsLog) switchDataFile() {
	var err error
	dl.dataFile.Close()
	dl.dataFileMaxN++
	dl.dataFile, err = os.OpenFile(dl.GetDataFilePath(dl.dataFileMaxN),
		os.O_RDWR|os.O_CREATE|os.O_APPEND, 0700)
	if err != nil {
		log.Fatalln(err)
	}
}

func (dl *TsLog) testFileSize(file *os.File, maxsize int64) bool {
	fileinfo, err := file.Stat()
	if err != nil {
		log.Fatalln(err)
	}
	return fileinfo.Size() >= maxsize
}
