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

func TestNewBitCaskDB(t *testing.T) {
	options := &Options{
		DBDirPath: "D:/temp/kvdb",
	}
	db := NewBitCaskDB(options)
	err := db.Run()
	if err != nil {
		t.Errorf("数据库启动失败")
	}
	fmt.Println("数据库启动成功", db.options.DBDirPath)
}

// 测试加载 String 类型数据
func TestLoadStringLogData(t *testing.T) {
	db := DefaultBitCaskDB()
	err := db.Run()
	if err != nil {
		t.Errorf("err: %v\n", err)
		return
	}
	ok := db.Set("he.wenyao", "is cool11")
	print(ok)
	value, err := db.Get("he.wenyao")
	if err != nil {
		t.Errorf("err: %v\n", err)
	}
	fmt.Printf("value: %v\n", value)
}
