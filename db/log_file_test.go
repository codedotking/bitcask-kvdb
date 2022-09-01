package db

import (
	"fmt"
	"testing"

	"github.com/he-wen-yao/bitcask-kvdb/util"
)

// TestNewLogFile 测试创建日志文件
func TestNewLogFile(t *testing.T) {
	_, err := NewLogFile("D:\\temp\\kvdb", STR_TYPE)
	if err != nil {
		t.Errorf("创建日志文件失败 %s", err)
		return
	}
	t.Log("日志创建成功")
}

// TestAppendLogEntry 测试追加日志文件
func TestAppendLogEntry(t *testing.T) {
	logFile, err := NewLogFile("D:\\temp\\kvdb", STR_TYPE)
	if err != nil {
		t.Errorf("创建日志文件失败 %s", err)
		return
	}
	for i := 0; i < 1000000; i++ {
		logEntry := NewLogEntry(util.RandomEnString(i%15), util.RandomEnString(i%100), uint16(STR_TYPE))
		err := logFile.AppendEntry(logEntry)
		if err != nil {
			t.Errorf("追加日志失败 %s", err)
			return
		}
	}
	fmt.Printf("logFile.offset: %v\n", logFile.offset)
}

// TestDeleteLogFile 测试删除日志文件
func TestDeleteLogFile(t *testing.T) {
	logFile, err := NewLogFile("D:\\temp\\kvdb", STR_TYPE)
	if err != nil {
		t.Errorf("创建日志文件失败 %s", err)
		return
	}
	err = logFile.Remove()
	if err != nil {
		t.Errorf("删除失败 %s", err)
		return
	}
	t.Logf("删除成功")
}

// TestLogFile_ToOlderLogFile 测试日志文件转为 Older 日志文件
func TestLogFile_ToOlderLogFile(t *testing.T) {
	logFile, err := NewLogFile("..", STR_TYPE)
	if err != nil {
		t.Errorf("创建日志文件失败 %s", err)
		return
	}
	err = logFile.ToOlderLogFile()
	if err != nil {
		t.Errorf("转为 Older 日志文件失败 %s", err)
		return
	}
	t.Logf("删除成功")
}
