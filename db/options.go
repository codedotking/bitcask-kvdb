package db

import (
	"fmt"
	"path/filepath"
)

type Options struct {
	DBPath string
}

// DefaultOptions 获取默认的配置选项
func DefaultOptions() *Options {
	dbPath := filepath.Join("/tmp", "rose")
	fmt.Println(dbPath)
	return &Options{DBPath: dbPath}
}
