package db

import "testing"

func TestLogEntryEncodeDecode(t *testing.T) {
	logEntry := NewLogEntry("你好", "你好", 0)
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
