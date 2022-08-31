package util

import (
	"os"
)

// PathExist 判断目录和文件是否存在
func PathExist(path string) bool {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	}
	return true
}

// CreateFile 创建一个文件
func CreateFile(fileName string) (file *os.File, err error) {
	return os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
}

func CloseFile(file *os.File) {
	_ = file.Close()
}
