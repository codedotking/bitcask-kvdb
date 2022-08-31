package db

import (
	"os"
	"sync"

	"github.com/he-wen-yao/bitcask-kvdb/util"
)

type bitCaskDB struct {
	mu      *sync.RWMutex
	options *Options
	// string 类型实现
	str_ *RadixTree
	// 活跃的文件
	activeLogFiles map[logType]*logFile
}

// DefaultBitCaskDB 创建一个 bitCaskDB 实例
func DefaultBitCaskDB() *bitCaskDB {
	return &bitCaskDB{
		options: DefaultOptions(),
		mu:      new(sync.RWMutex),
		str_:    &RadixTree{},
	}
}

// NewBitCaskDB 根据 Options 配置创建 bitCaskDB 实列
func NewBitCaskDB(options *Options) *bitCaskDB {
	return &bitCaskDB{
		options: options,
		mu:      new(sync.RWMutex),
		str_:    &RadixTree{},
	}
}

// Run 运行实例
func (db *bitCaskDB) Run() error {
	// 如果不存在此目录则创建
	if !util.PathExist(db.options.DBPath) {
		if err := os.MkdirAll(db.options.DBPath, os.ModePerm); err != nil {
			return err
		}
	}
	return nil
}

// CreateLogFile 创建日志文件
func (db *bitCaskDB) CreateLogFile(logType logType) error {
	// 如果不存在此目录则创建
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.activeLogFiles[logType] != nil {
		return nil
	}
	file, err := NewLogFile(db.options.DBPath, logType)
	if err != nil {
		return err
	}
	db.activeLogFiles[logType] = file
	return nil
}

// AppendLog 向日志文件中追加日志
func (db *bitCaskDB) AppendLog(log string, logType logType) error {
	// 创建 logType 对应的 日志文件
	if err := db.CreateLogFile(logType); err != nil {
		return err
	}
	// 获取 logType 对应的 activeLogFile
	activeLogFile := db.activeLogFiles[logType]
	err := activeLogFile.AppendEntry(NewLogEntry("log", "log", uint16(logType)))
	if err != nil {
		return err
	}
	return nil
}
