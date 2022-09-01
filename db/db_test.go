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

func TestLoadLogFiles(t *testing.T) {
	db := DefaultBitCaskDB()
	err := db.loadLogFiles()
	if err != nil {
		t.Errorf("加载日志失败")
		return
	}
	t.Logf("加载日志成功")

}

func TestReadLogEntry(t *testing.T) {
	db := DefaultBitCaskDB()
	err := db.Run()
	if err != nil {
		t.Errorf("数据库启动失败 %v", err)
		return
	}
	le, err := db.RedLogEntry(STR_TYPE, 0)
	if err != nil {
		t.Errorf("读取日志失败 %v", err)
		return
	}
	t.Logf(le.ToString())
	offset := le.GetSize()
	for err == nil || le != nil {
		le, err = db.RedLogEntry(STR_TYPE, offset)
		if le != nil {
			t.Logf(le.ToString())
			offset += le.GetSize()
		}
	}
}
