package db

import (
	"errors"
	"os"
	"sync"
	"sync/atomic"

	"github.com/he-wen-yao/bitcask-kvdb/util"
)

type logType int

// 日志类型定义
const (
	strType logType = iota
	listType
)

// 定义错误信息
var (
	// 写入数据大小不一致错误
	ErrWriteSizeNotEqual = errors.New("logfile: write size is not equal to entry size")
)

const (
	// InitialLogFileId initial log file id: 0.
	InitialLogFileId = 0
	// 日志文件的前缀
	FilePrefix = "kv."
	// 日志文件的后缀
	FileSuffix = ".data"
)

var LogType2FileName = map[logType]string{
	strType: "string",
}

// 日志文件
type logFile struct {
	// 读写锁
	mu sync.RWMutex
	// 实际的日志文件
	file *os.File
	// 偏移量记录当前日志写到哪里
	offset int64
}

// NewLogFile 根据目录和日志类型创建日志文件
func NewLogFile(filePath string, logType logType) (lf *logFile, err error) {
	fileName := filePath + "/" + FilePrefix + LogType2FileName[logType] + FileSuffix
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

// AppendEntry 向当前日志文件追加日志记录
func (file *logFile) AppendEntry(logEntry *logEntry) error {
	file.mu.Lock()
	defer file.mu.Unlock()
	buf, _ := logEntry.Encode()
	if len(buf) <= 0 {
		return nil
	}
	offset := atomic.LoadInt64(&file.offset)
	// 将日志记录写道指定位置
	n, err := file.file.WriteAt(buf, offset)
	if err != nil {
		return err
	}
	if n != len(buf) {
		return ErrWriteSizeNotEqual
	}
	atomic.AddInt64(&file.offset, int64(n))
	return nil
}
