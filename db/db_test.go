package db

import (
	"fmt"
	"testing"
)

func TestDBRun(t *testing.T) {
	// 默认会在项目根目录下创建 kvdb 目录
	db := DefaultBitCaskDB()
	err := db.Run()
	if err != nil {
		t.Errorf("数据库启动失败 %v", err)
		return
	}
	fmt.Println("数据库启动成功", db.options.DBDirPath)
}
