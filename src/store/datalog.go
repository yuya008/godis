package store

import (
	"bufio"
	ds "data_struct"
	"db"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/Terry-Mao/goconf"
	"io"
	"log"
	"os"
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

func (dl *DataLog) PutADbKey(db db.DB, key string, obj *ds.Object) {
	buffer := obj.GetBuffer()
	newBuffer := make([]byte, len(buffer))
	copy(newBuffer, buffer)
	dl.logWriteQ <- &dataRecord{
		witchdb:    db.Id,
		key0:       key,
		objectType: obj.GetObjectType(),
		objectData: newBuffer,
		inqueueT:   time.Now(),
	}
}

func (dl *DataLog) writeThread() {
	for {
		// dl.logWriteQ
	}
}

func (dl *DataLog) writeThreadSync() {
	t := time.Tick(dl.dataSyncInterval)
	for {
		select {
		case <-t:
			dl.CurFile.Sync()
		default:
			time.Sleep(1 * time.Millisecond)
		}
	}
}

func (dl *DataLog) StartDataWriteThread() {
	go dl.writeThread()
	go dl.writeThreadSync()
}

func loadAStringObject(reader io.Reader, witchdb uint16, dbs []db.DB) {
	db := dbs[witchdb]
	var (
		keylen   uint64
		valuelen uint64
	)
	err := binary.Read(reader, binary.BigEndian, &keylen)
	if err != nil {
		log.Fatalln(Err_data_format_fail)
	}
	if keylen <= 0 {
		log.Fatalln(Err_data_format_fail)
	}
	keybuf := make([]byte, keylen)
	_, err = reader.Read(keybuf)
	if err != nil {
		log.Fatalln(Err_data_format_fail)
	}

	err = binary.Read(reader, binary.BigEndian, &valuelen)
	if err != nil {
		log.Fatalln(Err_data_format_fail)
	}
	if keylen <= 0 {
		log.Fatalln(Err_data_format_fail)
	}
	valuebuf := make([]byte, valuelen)
	_, err = reader.Read(valuebuf)
	if err != nil {
		log.Fatalln(Err_data_format_fail)
	}
	db.SetDbKey(string(keybuf), ds.CreateStringObject(valuebuf, ds.NonTs))
}

func loadADataFile(file *os.File, dbs []db.DB) {
	reader := bufio.NewReader(file)
	var (
		maxdblen = uint16(len(dbs))
		witchdb  uint16
		keyType  uint8
	)
	for {
		err := binary.Read(reader, binary.BigEndian, &witchdb)
		if err != nil {
			log.Fatalln(Err_data_format_fail)
		}
		if witchdb >= maxdblen {
			log.Fatalln(Err_maxdbn_small)
		}
		err = binary.Read(reader, binary.BigEndian, &keyType)
		if err != nil {
			log.Fatalln(Err_data_format_fail)
		}
		switch keyType {
		case ds.STRING:
			loadAStringObject(reader, witchdb, dbs)
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
