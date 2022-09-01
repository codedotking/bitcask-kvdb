package db

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/he-wen-yao/bitcask-kvdb/util"
)

// 用来定义数据库所支持的日志类型
type logType int

const (
	STR_TYPE logType = iota
	LIST_TYPE
	// 日志文件的权限 111
	LOG_FILE_PERM = 0644
	// LOG_FILE_PREFIX 日志文件的前缀
	LOG_FILE_PREFIX = "kv"
	// LOG_FILE_SUFFIX 日志文件的后缀
	LOG_FILE_SUFFIX = "data"
)

// 定义错误信息
var (
	// ErrWriteSizeNotEqual 写入数据大小不一致错误
	ErrWriteSizeNotEqual = errors.New("logfile: write size is not equal to entry size")

	LogType2FileName = map[logType]string{
		STR_TYPE: "string",
	}

	FileName2LogType = map[string]logType{
		"string": STR_TYPE,
	}
)

// 日志文件
type logFile struct {
	// 读写锁
	mu *sync.RWMutex
	// 实际的日志文件
	file *os.File
	// 偏移量记录当前日志写到哪里
	offset int64
}

// NewLogFile 根据目录和日志类型创建日志文件
func NewLogFile(filePath string, logType logType) (lf *logFile, err error) {
	fileName := filepath.Join(filePath, fmt.Sprintf("%s.%s.%s", LOG_FILE_PREFIX, LogType2FileName[logType], LOG_FILE_SUFFIX))
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
func (lf *logFile) AppendEntry(le *logEntry) error {
	lf.mu.Lock()
	defer lf.mu.Unlock()
	buf, _ := le.Encode()
	// 如果日志内容为空，不写入
	if len(buf) <= 0 {
		return nil
	}
	// 将日志记录写道指定位置
	n, err := lf.file.Write(buf)
	if err != nil {
		return err
	}
	// 如果实际写入和预计写入不一致，抛出错误
	if n != len(buf) {
		return ErrWriteSizeNotEqual
	}
	atomic.AddInt64(&lf.offset, int64(n))
	return nil
}

// Remove 移除当前日志文件
func (lf *logFile) Remove() error {
	lf.mu.Lock()
	defer lf.mu.Unlock()
	if err := lf.file.Close(); err != nil {
		return err
	}
	return os.Remove(lf.file.Name())
}

// ToOlderLogFile 转为旧的日志文件
func (lf *logFile) ToOlderLogFile() error {
	var (
		err     error
		dstFile *os.File
		srcInfo os.FileInfo
	)
	// 获取 lf 所在的目录以及文件名
	filePath, fileName := path.Split(lf.file.Name())
	// older logFile 所在目录
	olderPath := path.Join(filePath, "older")
	// 备份时需要检查有没有 older 目录
	if !util.PathExist(olderPath) {
		if err := os.MkdirAll(olderPath, os.ModePerm); err != nil {
			return err
		}
	}
	// older logFile 的文件名
	destFileName := path.Join(olderPath, fileName)
	if dstFile, err = os.Create(destFileName); err != nil {
		return err
	}
	defer util.CloseFile(dstFile)
	if _, err = io.Copy(dstFile, lf.file); err != nil {
		return err
	}
	if srcInfo, err = os.Stat(filePath); err != nil {
		return err
	}
	// 修改 older logFile 权限
	return os.Chmod(destFileName, srcInfo.Mode())
}

// ReadLogEntry 在日志文件中读取一条日志
func (lf *logFile) ReadLogEntry(offset int64) (*logEntry, error) {
	// 去除记录头
	headerBuf, err := lf.readBytes(offset, LOG_ENTRY_HEADER_SIZE)
	if err != nil {
		return nil, err
	}
	header, err := DecodeHeader(headerBuf)
	if err != nil {
		return nil, err
	}
	// 获取 key 和 value 的值
	buf, err := lf.readBytes(offset+LOG_ENTRY_HEADER_SIZE, int64(header.KeySize+header.ValueSize))
	if err != nil {
		return nil, err
	}
	header.Key = buf[0:header.KeySize]
	header.Value = buf[header.KeySize:]
	return header, nil
}

// ReadAllLogEntryFromStart 从头开始读取所有日志记录，读取时处理日志
func (lf *logFile) ReadAllLogEntryFromStart(process func(entry *logEntry, offset int64)) error {
	offset := int64(0)
	for {
		entry, err := lf.ReadLogEntry(offset)
		if err != nil {
			if err.Error() == "EOF" || entry == nil {
				return nil
			}
			return err
		}
		process(entry, offset)
		offset += entry.GetSize()
	}
}

// ReadLogEntry 读取长度为 n 字节数据
func (lf *logFile) readBytes(offset, n int64) (buf []byte, err error) {
	buf = make([]byte, n)
	_, err = lf.file.ReadAt(buf, offset)
	return
}
