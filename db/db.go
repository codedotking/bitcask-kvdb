package db

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
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
func (db *bitCaskDB) Run() error {
	// 如果不存在此目录则创建
	if !util.PathExist(db.options.DBDirPath) {
		if err := os.MkdirAll(db.options.DBDirPath, os.ModePerm); err != nil {
			return err
		}
	}

	err := db.loadOlderLogData()
	if err != nil {
		return err
	}

	// 初始化日志文件
	err = db.loadLogFiles()
	if err != nil {
		return err
	}

	return nil
}

// loadLogFiles 加载 bitCaskDB 所需要的日志文件
func (db *bitCaskDB) loadLogFiles() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	// 读取存放日志的目录
	files, err := os.ReadDir(db.options.DBDirPath)
	if err != nil {
		return err
	}
	for _, file := range files {
		fileName := file.Name()
		// 加载活跃日志
		if strings.HasPrefix(fileName, LOG_FILE_PREFIX) && strings.HasSuffix(fileName, LOG_FILE_SUFFIX) {
			nameSplits := strings.Split(fileName, ".")
			if err != nil {
				return err
			}
			lt := FileName2LogType[nameSplits[1]]
			lf, err := os.OpenFile(filepath.Join(db.options.DBDirPath, fileName), os.O_APPEND|os.O_RDWR|os.O_CREATE, LOG_FILE_PERM)
			if err != nil {
				return err
			}
			db.activeLogFiles[lt] = &logFile{file: lf}
		}
	}
	return nil
}

// 加载 String 类型的数据
func (db *bitCaskDB) loadOlderLogData() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	// 读取存放日志的目录
	olderFiles, err := os.ReadDir(filepath.Join(db.options.DBDirPath, "older"))
	if err != nil {
		return err
	}
	if len(olderFiles) == 0 {
		return nil
	}
	for _, file := range olderFiles {
		fileName := file.Name()
		if strings.HasPrefix(fileName, LOG_FILE_PREFIX) {
			nameSplits := strings.Split(fileName, ".")
			lt := FileName2LogType[nameSplits[1]]
			n, _ := strconv.Atoi(nameSplits[3])
			db.olderFidNum[lt] = uint8(n)
			if lt == STR_TYPE {
				lf, err := os.OpenFile(filepath.Join(db.options.DBDirPath, "older", fileName), os.O_APPEND|os.O_RDWR|os.O_CREATE, LOG_FILE_PERM)
				if err != nil {
					return err
				}
				logF := &logFile{file: lf, offset: 0, mu: new(sync.RWMutex)}
				dst := db.strIndex.tree

				lee, _ := logF.ReadLogEntry(0)
				print(lee)
				// 加载所有日志记录
				err = logF.ReadAllLogEntryFromStart(func(entry *logEntry, offset int64) {
					if entry.OptType == OPT_ADD {
						dst.Put(string(entry.Key), Value{fid: uint8(n), offset: offset})
					} else if entry.OptType == OPT_DEL {
						dst.Delete(string(entry.Key))
					}
				})
				if err != nil {
					return err
				}
			}
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
