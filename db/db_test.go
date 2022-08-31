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

func TestLoadLogFiles(t *testing.T) {
	db := DefaultBitCaskDB()
	err := db.loadLogFiles()
	if err != nil {
		t.Errorf("加载日志失败")
		return
	}
	t.Logf("加载日志成功")

}

func TestBitCaskDBAppendLog(t *testing.T) {
	db := DefaultBitCaskDB()
	err := db.Run()
	if err != nil {
		t.Errorf("数据库启动失败")
		return
	}
	err = db.AppendLog("鸟", strType)
	if err != nil {
		t.Errorf("写入日志失败")
		return
	}
	t.Log("写入成功")
}
