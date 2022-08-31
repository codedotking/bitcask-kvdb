package db

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"

	"github.com/he-wen-yao/bitcask-kvdb/util"
)

type logType int

const (
	strType logType = iota
	listType
)

const (
	// InitialLogFileId initial log file id: 0.
	InitialLogFileId = 0
	// FilePrefix log file prefix.
	FilePrefix = "kv."
	FileSuffix = ".data"
)

var LogType2FileName = map[logType]string{
	strType: "string",
}

// 日志文件
type logFile struct {
	mu     sync.RWMutex
	file   *os.File
	offset int64
}

// NewLogFile 根据目录和日志类型创建日志文件
func NewLogFile(filePath string, logType logType) (lf *logFile, err error) {
	fileName := filePath + "/" + FilePrefix + LogType2FileName[logType] + FileSuffix
	print(fileName)
	f, err := util.CreateFile(fileName)
	if err != nil {
		return nil, err
	}
	stat, err := os.Stat(fileName)
	if err != nil {
		return nil, err
	}
	return &logFile{offset: stat.Size(), file: f}, nil
}

const entryHeaderSize = 10

// 一条日志记录的元数据
type logEntry struct {
	Key       []byte
	Value     []byte
	KeySize   uint32
	ValueSize uint32
	OptType   uint16 // 0 代表插入 1 代表修改 3 代表删除
}

// NewLogEntry 创建一个日志记录
func NewLogEntry(key, value []byte, optType uint16) *logEntry {
	return &logEntry{
		Key:       key,
		Value:     value,
		KeySize:   uint32(len(key)),
		ValueSize: uint32(len(value)),
		OptType:   optType,
	}
}

func (e *logEntry) GetSize() int64 {
	return int64(entryHeaderSize + e.KeySize + e.ValueSize)
}

// Encode 编码 Entry，返回字节数组
func (e *logEntry) Encode() ([]byte, error) {
	buf := make([]byte, e.GetSize())
	binary.BigEndian.PutUint32(buf[0:4], e.KeySize)
	binary.BigEndian.PutUint32(buf[4:8], e.ValueSize)
	binary.BigEndian.PutUint16(buf[8:10], e.OptType)
	copy(buf[entryHeaderSize:entryHeaderSize+e.KeySize], e.Key)
	copy(buf[entryHeaderSize+e.KeySize:], e.Value)
	return buf, nil
}

// ToString 打印日志记录
func (e *logEntry) ToString() string {
	return fmt.Sprintf("[optType = %d,key = %s, value = %s]", e.OptType, string(e.Key), string(e.Value))
}

// Decode 解码 buf 字节数组，返回 Entry
func Decode(buf []byte) (*logEntry, error) {
	ks := binary.BigEndian.Uint32(buf[0:4])
	vs := binary.BigEndian.Uint32(buf[4:8])
	optType := binary.BigEndian.Uint16(buf[8:10])
	key := buf[entryHeaderSize : entryHeaderSize+ks]
	value := buf[entryHeaderSize+ks:]
	return &logEntry{KeySize: ks, ValueSize: vs, OptType: optType, Key: key, Value: value}, nil
}
