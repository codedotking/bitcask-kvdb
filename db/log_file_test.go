package db

import (
	"fmt"
	"testing"

	"github.com/he-wen-yao/bitcask-kvdb/util"
)

// 测试创建日志文件
func TestNewLogFile(t *testing.T) {
	_, err := NewLogFile("D:\\temp\\kvdb", strType)
	if err != nil {
		t.Errorf("创建日志文件失败 %s", err)
		return
	}
	t.Log("日志创建成功")
}

// 测试追加日志文件
func TestAppendLogEntry(t *testing.T) {
	logFile, err := NewLogFile("D:\\temp\\kvdb", strType)
	if err != nil {
		t.Errorf("创建日志文件失败 %s", err)
		return
	}
	for i := 0; i < 1000000; i++ {
		logEntry := NewLogEntry(util.RandomEnString(i%15), util.RandomEnString(i%100), uint16(strType))
		logFile.AppendEntry(logEntry)
	}
	fmt.Printf("logFile.offset: %v\n", logFile.offset)
}
