package db

import "testing"

// 测试 String 中 SET 命令
func TestString_SET(t *testing.T) {
	db := DefaultBitCaskDB()
	err := db.Run()
	if err != nil {
		t.Errorf("启动失败 %v", err)
		return
	}
	db.Set("test", "ddd")
}
