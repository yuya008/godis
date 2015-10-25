package store

import (
	ds "data_struct"
	"db"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/Terry-Mao/goconf"
	"log"
	"os"
	"sync"
	"time"
)

/*
	data log file data_0 ... data_n
*/
const (
	DataFilePrefix         = "data_%d"
	DefaultDataFileMaxSize = 1000000000
	DefaultWriteQSize      = 1024
	DefaultWriteSync       = time.Millisecond * 1000
)

const (
	None = iota
	Del
)

var (
	Err_datadir_not_dir  = errors.New("datadir not is dir!")
	Err_data_format_fail = errors.New("data format fail!")
	Err_maxdbn_small     = errors.New("'max_dbs' too small cannot be loaded!")
)

type dataRecord struct {
	witchdb    uint16
	key0       string
	key1       string
	objectType uint8
	op         int8
	objectData []byte
	inqueueT   time.Time
}

type DataLog struct {
	Datadir          string
	CurMaxFileNo     int
	CurMinFileNo     int
	CurrentDir       *os.File
	CurFile          *os.File
	DataFileMaxSize  uint64
	logWriteQ        chan *dataRecord
	dataSyncInterval time.Duration
	filelock         sync.Mutex
}

func NewDataLog(sec *goconf.Section) (*DataLog, error) {
	var (
		datadir         string
		dataFileMaxSize uint64        = DefaultDataFileMaxSize
		dataWQSize      int           = DefaultWriteQSize
		dataSync        time.Duration = DefaultWriteSync
	)
	if v, err := sec.String("datadir"); err == nil {
		datadir = v
	}
	if v, err := sec.Int("data_file_max_size"); err == nil {
		dataFileMaxSize = uint64(v)
	}
	if v, err := sec.Int("data_write_queue_size"); err == nil {
		dataWQSize = int(v)
	}
	if v, err := sec.Int("data_write_sync_interval"); err == nil {
		dataSync = time.Millisecond * time.Duration(v)
	}

	fileinfo, err := os.Stat(datadir)
	if err != nil {
		return nil, err
	}
	if !fileinfo.IsDir() {
		return nil, Err_datadir_not_dir
	}
	dl := &DataLog{
		Datadir:          datadir,
		DataFileMaxSize:  dataFileMaxSize,
		logWriteQ:        make(chan *dataRecord, dataWQSize),
		dataSyncInterval: dataSync,
	}
	err = dl.scanLogFile()
	if err != nil {
		return nil, err
	}
	return dl, nil
}

func (dl *DataLog) GetDataFilePath(i int) string {
	return fmt.Sprintf("%s/%s", dl.Datadir, fmt.Sprintf(DataFilePrefix, i))
}

func (dl *DataLog) PutKeyValue(db *db.DB, key string, op int8, obj *ds.Object) {
	dl.PutKeyKeyValue(db, key, "", op, obj)
}

func (dl *DataLog) PutKeyKeyValue(
	db *db.DB,
	key0 string,
	key1 string,
	op int8, obj *ds.Object) {
	var newBuffer []byte
	if op == None {
		buffer := obj.GetBuffer()
		newBuffer = make([]byte, len(buffer))
		copy(newBuffer, buffer)
	}
	dl.logWriteQ <- &dataRecord{
		witchdb:    db.Id,
		key0:       key0,
		key1:       key1,
		op:         op,
		objectType: obj.GetObjectType(),
		objectData: newBuffer,
		inqueueT:   time.Now(),
	}
}

func (dl *DataLog) testDataFileSize() bool {
	fi, err := dl.CurFile.Stat()
	if err != nil {
		log.Fatalln(err)
	}
	return fi.Size() < int64(dl.DataFileMaxSize)
}

func (dl *DataLog) switchDataFile() {
	var err error
	dl.filelock.Lock()
	defer dl.filelock.Unlock()
	dl.CurFile.Close()
	dl.CurMaxFileNo++
	dl.CurFile, err = os.OpenFile(dl.GetDataFilePath(dl.CurMaxFileNo),
		os.O_RDWR|os.O_CREATE, 0700)
	if err != nil {
		log.Fatalln(err)
	}
}

func (dl *DataLog) writeAkey(f *os.File, key string) {
	var keylen uint64 = uint64(len(key))
	err := binary.Write(f, binary.BigEndian, keylen)
	if err != nil {
		log.Fatalln(err)
	}
	log.Println("writeAkey keylen", keylen)
	log.Println("writeAkey key", key)
	_, err = f.Write([]byte(key))
	if err != nil {
		log.Fatalln(err)
	}
}
func (dl *DataLog) writeKeyStatus(f *os.File, s int8) {
	err := binary.Write(f, binary.BigEndian, s)
	if err != nil {
		log.Fatalln(err)
	}
}

func (dl *DataLog) writeThread() {
	for {
		if !dl.testDataFileSize() {
			dl.switchDataFile()
		}
		dr, ok := <-dl.logWriteQ
		if !ok {
			continue
		}
		// 写入db号
		log.Println("writeThread witchdb", dr.witchdb)
		err := binary.Write(dl.CurFile, binary.BigEndian, dr.witchdb)
		if err != nil {
			log.Fatalln(err)
		}
		// 写入对象类型
		log.Println("writeThread objectType", dr.objectType)
		err = binary.Write(dl.CurFile, binary.BigEndian, dr.objectType)
		if err != nil {
			log.Fatalln(err)
		}
		log.Println("writeThread op", dr.op)
		if dr.op == None {
			var valuelen uint64 = uint64(len(dr.objectData))
			if dr.key1 == "" {
				dl.writeAkey(dl.CurFile, dr.key0)
				dl.writeKeyStatus(dl.CurFile, None)
				err = binary.Write(dl.CurFile, binary.BigEndian, valuelen)
				if err != nil {
					log.Fatalln(err)
				}
				_, err = dl.CurFile.Write(dr.objectData)
				if err != nil {
					log.Fatalln(err)
				}
			} else {
				dl.writeAkey(dl.CurFile, dr.key0)
				dl.writeKeyStatus(dl.CurFile, None)
				dl.writeAkey(dl.CurFile, dr.key1)
				dl.writeKeyStatus(dl.CurFile, None)
				err = binary.Write(dl.CurFile, binary.BigEndian, valuelen)
				if err != nil {
					log.Fatalln(err)
				}
				_, err = dl.CurFile.Write(dr.objectData)
				if err != nil {
					log.Fatalln(err)
				}
			}
		} else {
			if dr.key1 == "" {
				dl.writeAkey(dl.CurFile, dr.key0)
				dl.writeKeyStatus(dl.CurFile, Del)
			} else {
				dl.writeAkey(dl.CurFile, dr.key0)
				dl.writeKeyStatus(dl.CurFile, None)
				dl.writeAkey(dl.CurFile, dr.key1)
				dl.writeKeyStatus(dl.CurFile, Del)
			}
		}
	}
}

func (dl *DataLog) writeThreadSync() {
	t := time.Tick(dl.dataSyncInterval)
	for {
		select {
		case <-t:
			dl.filelock.Lock()
			dl.CurFile.Sync()
			dl.filelock.Unlock()
		default:
			time.Sleep(1 * time.Millisecond)
		}
	}
}

func (dl *DataLog) StartDataWriteThread() {
	go dl.writeThread()
	go dl.writeThreadSync()
}

func loadAStringObject(reader *os.File, witchdb uint16, dbs []db.DB) int64 {
	db := dbs[witchdb]
	var (
		keylen   uint64
		valuelen uint64
		op       int8
		curSize  int64
	)
	// 读取键长度
	err := binary.Read(reader, binary.BigEndian, &keylen)
	if err != nil {
		log.Fatalln(Err_data_format_fail)
	}
	if keylen <= 0 {
		log.Fatalln(Err_data_format_fail)
	}
	log.Println("loadAStringObject keylen", keylen)
	curSize += 8
	// 读取键内容
	keybuf := make([]byte, keylen)
	_, err = reader.Read(keybuf)
	if err != nil {
		log.Fatalln(Err_data_format_fail)
	}
	curSize += int64(keylen)
	log.Println("loadAStringObject keybuf", keybuf)
	// 读取键操作
	err = binary.Read(reader, binary.BigEndian, &op)
	if err != nil {
		log.Fatalln(Err_data_format_fail)
	}
	curSize += 1
	switch op {
	case Del:
		db.DeleteKey(string(keybuf))
		return curSize
	case None:
	default:
		log.Fatalln(Err_data_format_fail)
	}
	// 读取值长度
	err = binary.Read(reader, binary.BigEndian, &valuelen)
	if err != nil {
		log.Fatalln(Err_data_format_fail)
	}
	if keylen <= 0 {
		log.Fatalln(Err_data_format_fail)
	}
	curSize += 8
	log.Println(valuelen)
	// 读取值内容
	valuebuf := make([]byte, valuelen)
	_, err = reader.Read(valuebuf)
	if err != nil {
		log.Fatalln(Err_data_format_fail)
	}
	curSize += int64(valuelen)
	db.SetDbKey(string(keybuf), ds.CreateStringObject(valuebuf, ds.NonTs))
	return curSize
}

func loadADataFile(file *os.File, dbs []db.DB) {
	fileinfo, err := file.Stat()
	if err != nil {
		log.Fatalln(err)
	}
	var (
		maxdblen  = uint16(len(dbs))
		witchdb   uint16
		keyType   uint8
		totalSize int64 = fileinfo.Size()
		curSize   int64
	)
	for curSize < totalSize {
		// 读取所属DB
		err := binary.Read(file, binary.BigEndian, &witchdb)
		if err != nil {
			log.Fatalln(Err_data_format_fail)
		}
		curSize += 2
		log.Println("loadADataFile witchdb", witchdb)
		if witchdb >= maxdblen {
			log.Fatalln(Err_maxdbn_small)
		}
		// 读取键类型
		err = binary.Read(file, binary.BigEndian, &keyType)
		if err != nil {
			log.Fatalln(Err_data_format_fail)
		}
		log.Println("loadADataFile keyType", keyType)
		curSize += 1
		switch keyType {
		case ds.STRING:
			curSize += loadAStringObject(file, witchdb, dbs)
		default:
			log.Fatalln("Unknown object type")
		}
	}
}
func (dl *DataLog) LoadDiskData(dbs []db.DB) {
	for no := dl.CurMinFileNo; no <= dl.CurMaxFileNo; no++ {
		file, err := os.Open(dl.GetDataFilePath(no))
		if err != nil {
			log.Fatalln(err)
		}
		if fileinfo, err := file.Stat(); err != nil {
			continue
		} else if fileinfo.Size() == 0 {
			continue
		}
		loadADataFile(file, dbs)
		file.Close()
	}
}

func (dl *DataLog) scanLogFile() error {
	var err error
	dl.CurrentDir, err = os.Open(dl.Datadir)
	if err != nil {
		return err
	}
	files, err := dl.CurrentDir.Readdirnames(0)
	if err != nil {
		return err
	}
	var (
		fileN      int
		firstfound bool = true
	)
	for _, file := range files {
		_, err = fmt.Sscanf(file, DataFilePrefix, &fileN)
		if err != nil {
			continue
		}
		if fileN > dl.CurMaxFileNo {
			dl.CurMaxFileNo = fileN
		} else if firstfound || fileN < dl.CurMinFileNo {
			dl.CurMinFileNo = fileN
			firstfound = false
		}
	}
	dl.CurFile, err = os.OpenFile(dl.GetDataFilePath(dl.CurMaxFileNo),
		os.O_RDWR|os.O_CREATE, 0700)
	if err != nil {
		return err
	}
	return nil
}
