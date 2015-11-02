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
	"sync/atomic"
	"utils"
)

const (
	TsLogMetaFilePrefix         = "tslogmeta_%d"
	TsLogFilePrefix             = "tslog_%d"
	DefaultTsLogFileMaxSize     = 1000000000
	DefaultTsLogMetaFileMaxSize = 1000000000
	MaxWriteFd                  = 128
)

const (
	NotCommit = iota
	Commit
	Committed
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

	metaFileMinN      int
	metaFileMaxN      int
	mutex0            sync.Mutex
	curMetaFileOffset int64
	mutex1            sync.Mutex
	wfilesN           uint32
}

type RecordPosition struct {
	FileNo uint16
	Offset int64
	File   *os.File
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
			log.Panicln("Please configure dataDir!")
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

func (dl *TsLog) skipAMeta(fd *os.File, curSize *int64) {
	var length uint16
	// 跳过id
	_, err := fd.Seek(8, os.SEEK_CUR)
	if err != nil {
		log.Panicln(err)
	}
	*curSize += 8
	err = binary.Read(fd, binary.BigEndian, &length)
	if err != nil {
		log.Panicln(err)
	}
	*curSize += 2
	positionLen := int64(length * 10)
	_, err = fd.Seek(positionLen, os.SEEK_CUR)
	if err != nil {
		log.Panicln(err)
	}
	*curSize += positionLen
}

func (dl *TsLog) Load(dbs []db.DB) {
	var (
		status    uint8
		totalSize int64
		curSize   int64
		offset    int64
	)
	for i := dl.metaFileMinN; i <= dl.metaFileMaxN; i++ {
		log.Println("打开了meta文件", i)
		fd, err := os.Open(dl.GetMetaFilePath(i))
		if err != nil {
			log.Panicln(err)
		}
		fileinfo, err := fd.Stat()
		if err != nil {
			log.Panicln(err)
		}
		totalSize = fileinfo.Size()
		log.Println("打开了meta文件总字节数", totalSize)
		for totalSize > curSize {
			offset, err = fd.Seek(0, os.SEEK_CUR)
			if err != nil {
				log.Panicln(err)
			}
			err = binary.Read(fd, binary.BigEndian, &status)
			if err != nil {
				log.Panicln(err)
			}
			curSize += 1
			log.Println("meta状态", status)
			if status == Commit {
				dl.rollBackCommitTs(fd, dbs, &curSize)
				dl.SetTsStatus(&RecordPosition{
					FileNo: uint16(i),
					Offset: offset,
				}, NotCommit)
			} else {
				dl.skipAMeta(fd, &curSize)
			}
		}
		fd.Close()
	}
}

func (dl *TsLog) loadARowData(fileNo uint16, offset int64, dbs []db.DB) {
	file, err := os.Open(dl.GetDataFilePath(int(fileNo)))
	defer file.Close()
	if err != nil {
		log.Panicln(err)
	}
	_, err = file.Seek(offset, os.SEEK_SET)
	if err != nil {
		log.Panicln(err)
	}
	var (
		dbid    uint16
		objtype uint8
	)
	err = binary.Read(file, binary.BigEndian, &dbid)
	if err != nil {
		log.Panicln(err)
	}
	log.Println("loadARowData()", dbid)
	err = binary.Read(file, binary.BigEndian, &objtype)
	if err != nil {
		log.Panicln(err)
	}
	log.Println("loadARowData()", objtype)
	switch objtype {
	case ds.STRING:
		if dbid >= uint16(len(dbs)) {
			log.Panicln("dbs too small")
		}
		dbs[dbid].LoadDbObjFromReader(file, ds.STRING)
	default:
		log.Panicln("unknow obj type")
	}
}

func (dl *TsLog) rollBackCommitTs(fd *os.File, dbs []db.DB, curSize *int64) {
	var (
		tsid   uint64
		length uint16
		i      uint16
		fileNo uint16
		offset int64
	)
	err := binary.Read(fd, binary.BigEndian, &tsid)
	if err != nil {
		log.Panicln(err)
	}
	*curSize += 8
	log.Println("meta tsid", tsid)
	err = binary.Read(fd, binary.BigEndian, &length)
	if err != nil {
		log.Panicln(err)
	}
	*curSize += 2
	log.Println("meta length", length)
	for ; i < length; i++ {
		err = binary.Read(fd, binary.BigEndian, &fileNo)
		if err != nil {
			log.Panicln(err)
		}
		*curSize += 2
		log.Println("meta fileNo", fileNo)
		err = binary.Read(fd, binary.BigEndian, &offset)
		if err != nil {
			log.Panicln(err)
		}
		*curSize += 8
		log.Println("meta offset", offset)
		dl.loadARowData(fileNo, offset, dbs)
	}
}

func (rp *RecordPosition) open(dl *TsLog) {
	if atomic.AddUint32(&dl.wfilesN, 1) > MaxWriteFd {
		log.Panicln("wfilesN upper limit is reached!")
	}
	var err error
	rp.File, err = os.OpenFile(dl.GetMetaFilePath(int(rp.FileNo)),
		os.O_RDWR, 0600)
	if err != nil {
		log.Panicln(err)
	}
	_, err = rp.File.Seek(rp.Offset, os.SEEK_SET)
	if err != nil {
		log.Panicln(err)
	}
}

func (rp *RecordPosition) destroy(dl *TsLog) {
	rp.File.Close()
	atomic.AddUint32(&dl.wfilesN, ^uint32(0))
}

func (dl *TsLog) SetTsStatus(rp *RecordPosition, s uint8) {
	if rp == nil {
		return
	}
	rp.open(dl)
	err := binary.Write(rp.File, binary.BigEndian, &s)
	if err != nil {
		log.Panicln(err)
	}
	rp.destroy(dl)
}

func (dl *TsLog) scanTsLogFile() error {
	dir, err := os.Open(dl.dir)
	if err != nil {
		log.Panicln(err)
	}
	files, err := dir.Readdirnames(0)
	if err != nil {
		return err
	}
	dl.metaFileMaxN, dl.metaFileMinN = utils.FindFileNMaxAndMin(files, TsLogMetaFilePrefix)
	dl.metaFile, err = os.OpenFile(dl.GetMetaFilePath(dl.metaFileMaxN),
		os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	dl.dataFileMaxN, dl.dataFileMinN = utils.FindFileNMaxAndMin(files, TsLogFilePrefix)
	dl.dataFile, err = os.OpenFile(dl.GetDataFilePath(dl.dataFileMaxN),
		os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	log.Println(dl)
	dir.Close()
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
	list ds.List,
) *RecordPosition {
	if list.Len() == 0 {
		return nil
	}
	buffer := bytes.NewBuffer(nil)
	err := binary.Write(buffer, binary.BigEndian, &status)
	if err != nil {
		return nil
	}
	err = binary.Write(buffer, binary.BigEndian, &id)
	if err != nil {
		return nil
	}
	var tn uint16 = uint16(list.Len())
	err = binary.Write(buffer, binary.BigEndian, &tn)
	if err != nil {
		return nil
	}
	log.Println("PutAMeta()", dl.metaFile)
	isswitchfile := dl.testFileSize(dl.metaFile, int64(dl.metaFileMaxSize))
	var wfilen uint16 = uint16(dl.metaFileMaxN)
	if isswitchfile {
		wfilen++
	}
	for e := list.GetFirstNode(); e != nil; e = e.Next {
		if v, ok := e.Value.(*RecordPosition); ok {
			log.Println(v)
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
	log.Println("PutAMeta()", isswitchfile)
	offset, err := dl.metaFile.Seek(0, os.SEEK_CUR)
	if err != nil {
		return nil
	}
	_, err = dl.metaFile.Write(buffer.Bytes())
	if err != nil {
		log.Println("PutAMeta()", err)
	}
	return &RecordPosition{
		FileNo: uint16(dl.metaFileMaxN),
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
	offset, err := dl.dataFile.Seek(0, os.SEEK_CUR)
	if err != nil {
		return nil
	}
	dl.dataFile.Write(buffer.Bytes())
	log.Println("tslog.Put() FileNo", dl.dataFileMaxN)
	log.Println("tslog.Put() Offset", offset)
	return &RecordPosition{
		FileNo: uint16(dl.dataFileMaxN),
		Offset: offset,
	}
}

func (dl *TsLog) switchMetaFile() {
	var err error
	dl.metaFile.Close()
	dl.metaFileMaxN++
	dl.metaFile, err = os.OpenFile(dl.GetMetaFilePath(dl.metaFileMaxN),
		os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		log.Panicln(err)
	}
}

func (dl *TsLog) switchDataFile() {
	var err error
	dl.dataFile.Close()
	dl.dataFileMaxN++
	dl.dataFile, err = os.OpenFile(dl.GetDataFilePath(dl.dataFileMaxN),
		os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		log.Panicln(err)
	}
}

func (dl *TsLog) testFileSize(file *os.File, maxsize int64) bool {
	fileinfo, err := file.Stat()
	if err != nil {
		log.Panicln("testFileSize", err)
	}
	return fileinfo.Size() >= maxsize
}
