package db

import (
	"path/filepath"
)

const (
	DB_DIR_NAME = "kvdb"
)

type Options struct {
	DBPath string

	// 日志文件最大值 默认 512 MB
	LogFileMaxSize uint64
}

// DefaultOptions 获取默认的配置选项
func DefaultOptions() *Options {
	return &Options{DBPath: filepath.Join("D:/temp", "kvdb"), LogFileMaxSize: 512 << 20}
}
