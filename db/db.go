package db

import (
	"io/fs"
	"os"
	"sync"

	"github.com/he-wen-yao/bitcask-kvdb/util"
)

type (
	bitCaskDB struct {
		mu      *sync.RWMutex
		options *Options
		// 记录每个类型的 older 个数
		olderFidNum map[logType]uint8
		// string 索引映射树
		strIndex *strIndex
		// 活跃的文件
		activeLogFiles map[logType]*logFile
		// 日志类型对应文件ID
		logTypeFIds map[logType][]int
	}
)

// DefaultBitCaskDB 创建一个 bitCaskDB 实例
func DefaultBitCaskDB() *bitCaskDB {
	return &bitCaskDB{
		options:        DefaultOptions(),
		mu:             new(sync.RWMutex),
		olderFidNum:    make(map[logType]uint8),
		strIndex:       NewStrIndex(),
		activeLogFiles: make(map[logType]*logFile),
	}
}

// NewBitCaskDB 根据 Options 配置创建 bitCaskDB 实列
func NewBitCaskDB(options *Options) *bitCaskDB {
	return &bitCaskDB{
		options:        options,
		mu:             new(sync.RWMutex),
		olderFidNum:    make(map[logType]uint8),
		strIndex:       NewStrIndex(),
		activeLogFiles: make(map[logType]*logFile),
	}
}

// Run 运行实例
func (db *bitCaskDB) Run() (err error) {
	dbLog.Printf("bitCaskDB start run .....")
	// 如果不存在此目录则创建
	err = db.CreateDirIfExist(db.options.DBDirPath)
	if err != nil {
		return
	}
	db.logTypeFIds = map[logType][]int{}
	err = db.loadStringLogData()
	return
}

// CreateLogFile 创建日志文件
func (db *bitCaskDB) CreateLogFile(logType logType) error {
	// 如果不存在此目录则创建
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.activeLogFiles[logType] != nil {
		return nil
	}
	file, err := NewLogFile(db.options.DBDirPath, logType)
	if err != nil {
		return err
	}
	db.activeLogFiles[logType] = file
	return nil
}

// AppendLog 向日志文件中追加日志
func (db *bitCaskDB) AppendLog(key, value string, logType logType, otType uint16) (*logFile, int64, error) {
	// 创建 logType 对应的 日志文件
	if err := db.CreateLogFile(logType); err != nil {
		return nil, 0, err
	}
	// 获取 logType 对应的 activeLogFile
	activeLogFile := db.activeLogFiles[logType]
	lf := NewLogEntry(key, value, otType)
	err := activeLogFile.AppendEntry(lf)
	if err != nil {
		return nil, 0, err
	}
	return activeLogFile, lf.GetSize(), nil
}

// RedLogEntry  读取指定日志类型的日志记录
func (db *bitCaskDB) RedLogEntry(lt logType, offset int64) (*logEntry, error) {
	le, err := db.activeLogFiles[lt].ReadLogEntry(offset)
	if err != nil {
		return nil, err
	}
	return le, nil
}

// CreateDirIfExist  如果目录不存在则创建，存在则忽略
func (db *bitCaskDB) CreateDirIfExist(dirPath string) (err error) {
	if !util.PathExist(dirPath) {
		if err = os.MkdirAll(dirPath, os.ModePerm); err != nil {
			return err
		}
	}
	return nil
}

// ReaderDir 读取目录中的内容
func (db *bitCaskDB) ReadDir(dirPath string) (files []fs.DirEntry, err error) {
	err = db.CreateDirIfExist(dirPath)
	if err != err {
		return nil, err
	}
	files, err = os.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}
	return
}
