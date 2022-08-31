package db

import (
	"testing"
)

func TestNewLogFile(t *testing.T) {
	_, err := NewLogFile("D:\\temp\\kvdb", strType)
	if err != nil {
		t.Errorf("创建日志文件失败 %s", err)
		return
	}
	t.Log("日志创建成功")
}

func TestLogEntryEncodeDecode(t *testing.T) {
	logEntry := NewLogEntry([]byte("你好"), []byte("你好"), 0)
	t.Logf("日志原型 %v", logEntry)
	res, err := logEntry.Encode()
	if err != nil {
		t.Errorf("加密失败")
		return
	}

	decode, err := Decode(res)
	if err != nil {
		t.Errorf("解密失败")
		return
	}

	t.Logf("解密成功 %s", decode.ToString())
}
