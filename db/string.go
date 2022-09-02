// Package db string 类型命令实现
package db

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

type strIndex struct {
	mu   *sync.RWMutex
	tree *RadixTree
}

// NewStrIndex 实例化一个 strIndex
func NewStrIndex() *strIndex {
	return &strIndex{mu: new(sync.RWMutex), tree: NewRadixTree()}
}

// loadStringLogData 加载 String 类型的 Log 数据
func (db *bitCaskDB) loadStringLogData() (err error) {
	dbLog.Printf("start load string log data")
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	// 先加载已经归档的日志 older 目录下的日志
	olderPath := filepath.Join(db.options.DBDirPath, "older", "string")
	files, err := db.ReadDir(olderPath)
	if err != nil {
		return
	}
	le := &logFile{mu: new(sync.RWMutex)}
	stringPrefix := fmt.Sprintf("%s.%s", LOG_FILE_PREFIX, LogType2FileName[STR_TYPE])
	// 遍历 older 日志文件
	for _, file := range files {
		fileName := file.Name()
		if !strings.HasPrefix(fileName, stringPrefix) {
			continue
		}
		filePath := filepath.Join(olderPath, file.Name())
		nameSplit := strings.Split(file.Name(), ".")
		if len(nameSplit) != 4 {
			continue
		}
		fid, err := strconv.ParseInt(nameSplit[3], 10, 64)
		if err != nil {
			// 不是数字，跳过此文件
			continue
		}
		// 以只读模式打开文件
		fd, err := os.OpenFile(filePath, os.O_RDONLY, LOG_FILE_PERM)
		if err != nil {
			return err
		}
		le.file = fd
		le.fid = fid
		// 读取文件内得所有记录
		db.updateStrIndexUseLogFile(le, true)
	}
	// 以追加模式打开  StrIndex 在使用的 日志文件
	lf, err := os.OpenFile(filepath.Join(db.options.DBDirPath, stringPrefix), os.O_APPEND|os.O_RDWR|os.O_CREATE, LOG_FILE_PERM)
	if err != nil {
		return
	}
	activeLogFile := &logFile{file: lf, fid: 0, mu: new(sync.RWMutex)}
	err = db.updateStrIndexUseLogFile(activeLogFile, false)
	if err != nil {
		return
	}
	db.activeLogFiles[STR_TYPE] = activeLogFile
	dbLog.Printf("log of string type load success")
	return
}

// updateStrIndexUseLogFile 使用 某一个日志文件刷新 索引信息 flag = true 需要关闭文件 false 不需要关闭
func (db *bitCaskDB) updateStrIndexUseLogFile(lf *logFile, flag bool) (err error) {
	// 加载完关掉
	if flag {
		defer lf.file.Close()
	}
	return lf.ReadAllLogEntryFromStart(func(e *logEntry, offset int64) {
		if e.OptType == OPT_ADD {
			v := Value{fid: lf.fid, entrySize: e.GetSize(), offset: offset}
			db.strIndex.tree.Put(string(e.Key), v)
		} else if e.OptType == OPT_DEL {
			db.strIndex.tree.Delete(string(e.Key))
		}
	})
}

// 在 String Log 中读取一个 LogEntry
func (db *bitCaskDB) readStringLogAt(fid int64, offset int64) (e *logEntry) {
	if fid == 0 {
		lf := db.activeLogFiles[STR_TYPE]
		e, _ = lf.ReadLogEntry(offset)
		return
	} else {
		stringPrefix := fmt.Sprintf("%s.%s", LOG_FILE_PREFIX, LogType2FileName[STR_TYPE])
		filePath := filepath.Join(db.options.DBDirPath, "order", "string", fmt.Sprintf("%s.%8d", stringPrefix, fid))
		fd, _ := os.OpenFile(filePath, os.O_RDONLY, LOG_FILE_PERM)
		lf := &logFile{file: fd}
		e, _ = lf.ReadLogEntry(offset)
		return
	}
}

func (db *bitCaskDB) Set(key, value string) bool {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	lf, entrySize, err := db.AppendLog(key, value, STR_TYPE, OPT_ADD)
	if err != nil {
		return false
	}
	// 更新内存索引
	_, ok := db.strIndex.tree.Put(key, Value{offset: lf.offset, entrySize: entrySize})
	return ok
}

func (db *bitCaskDB) Get(key string) (string, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	v := db.strIndex.tree.Get(key)
	if v == nil {
		return "", ErrKeyNotFound
	}
	v_, ok := v.(Value)
	if !ok {
		return "", ErrKeyNotFound
	}
	// TODO 去实际文件根据 offset 位置读取 key 对应的 Value
	e := db.readStringLogAt(v_.fid, v_.offset)
	if e == nil {
		return "", nil
	}
	print(e.ToString())
	// 更新内存索引
	return string(e.Value), nil
}

// Del 删除 key
func (db *bitCaskDB) Del(key string) bool {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	_, ok := db.strIndex.tree.Delete(key)
	return ok
}
