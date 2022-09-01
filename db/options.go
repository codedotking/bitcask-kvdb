package db

import (
	"path/filepath"
)

// DB_HOME_DIR_NAME 数据库存在的目录
const (
	DB_HOME_DIR_NAME  = "kvdb"
	LOG_FILE_MAX_SIZE = 512 << 20
)

type Options struct {
	// 数据库数据存放的目录
	DBDirPath string
	// 日志文件最大值 默认 512 MB
	LogFileMaxSize uint64
}

// DefaultOptions 获取默认的配置选项
func DefaultOptions() *Options {
	return &Options{
		DBDirPath:      filepath.Join("D:/temp", DB_HOME_DIR_NAME),
		LogFileMaxSize: LOG_FILE_MAX_SIZE,
	}
}
