package db

import (
	"os"
	"testing"
)

func TestLogEntryEncodeDecode(t *testing.T) {
	logEntry := NewLogEntry("A1111", "A1111111111111", 1)
	t.Logf("日志原型 %v", logEntry)
	res, err := logEntry.Encode()
	if err != nil {
		t.Errorf("加密失败")
		return
	}
	file, err := os.OpenFile("text.txt", os.O_APPEND|os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		t.Errorf("打开文件失败")
		return
	}
	err = file.Truncate(512 << 20)
	if err != nil {
		t.Errorf("err: %v\n", err)
		return
	}
	file.Write(res)
}
