package db

import (
	"errors"
	"io"
	"os"
	"path"
	"sync"
	"sync/atomic"

	"github.com/he-wen-yao/bitcask-kvdb/util"
)

type logType int

// 日志类型定义
const (
	strType logType = iota
)

// 定义错误信息
var (
	// ErrWriteSizeNotEqual 写入数据大小不一致错误
	ErrWriteSizeNotEqual = errors.New("logfile: write size is not equal to entry size")
)

const (
	// LogFilePrefix 日志文件的前缀
	LogFilePrefix = "kv."
	// LogFileSuffix 日志文件的后缀
	LogFileSuffix = ".data"
)

var LogType2FileName = map[logType]string{
	strType: "string",
}

var FileName2LogType = map[string]logType{
	"string": strType,
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
	fileName := filePath + "/" + LogFilePrefix + LogType2FileName[logType] + LogFileSuffix
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

// Remove 移除当前日志文件
func (file *logFile) Remove() error {
	file.mu.Lock()
	defer file.mu.Unlock()
	if err := file.file.Close(); err != nil {
		return err
	}
	return os.Remove(file.file.Name())
}

// ToOlderLogFile 转为旧的日志文件
func (file *logFile) ToOlderLogFile() error {
	var (
		err     error
		dstFile *os.File
		srcInfo os.FileInfo
	)
	srcFile := file.file
	filePath := srcFile.Name()
	dirPath, fileName := path.Split(filePath)
	olderPath := path.Join(dirPath, "older")
	if !util.PathExist(olderPath) {
		if err := os.MkdirAll(olderPath, os.ModePerm); err != nil {
			return err
		}
	}
	destFileName := path.Join(olderPath, fileName)
	if dstFile, err = os.Create(destFileName); err != nil {
		return err
	}
	defer util.CloseFile(dstFile)

	if _, err = io.Copy(dstFile, srcFile); err != nil {
		return err
	}
	if srcInfo, err = os.Stat(filePath); err != nil {
		return err
	}
	// 修改复制后的文件权限
	return os.Chmod(destFileName, srcInfo.Mode())
}
