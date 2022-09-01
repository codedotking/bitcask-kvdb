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
	err = db.Set("test", "ddd")
	if err != nil {
		t.Errorf("执行 Set 命令失败 %v", err)
		return
	}
	t.Logf("执行 SET 命令成功")

}
