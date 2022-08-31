package db

import (
	"fmt"
	"testing"
)

func TestDBRun(t *testing.T) {
	db := DefaultBitCaskDB()

	err := db.Run()
	if err != nil {
		t.Errorf("数据库启动失败")
	}
	fmt.Println("数据库启动成功", db.options.DBPath)
}

func TestNewBitCaskDB(t *testing.T) {
	options := &Options{
		DBPath: "D:/temp/kvdb",
	}
	db := NewBitCaskDB(options)
	err := db.Run()
	if err != nil {
		t.Errorf("数据库启动失败")
	}
	fmt.Println("数据库启动成功", db.options.DBPath)
}
