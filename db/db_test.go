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
		t.Errorf("数据库启动失败 %v", err)
		return
	}
	lf, size, err := db.AppendLog("AAAA", "ddd", STR_TYPE, OPT_ADD)
	if err != nil {
		t.Errorf("写入日志失败, %v", err)
		return
	}
	fmt.Printf("Value{offset: lf.offset, size: size}: %v\n", Value{offset: lf.offset, size: size, value: "ddd"})
	db.strIndex.Put("AAAA", Value{offset: lf.offset, size: size})
	value := db.strIndex.Get("AAAA")
	fmt.Printf("value.(Value): %v\n", value.(Value))
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
	offest := le.GetSize()
	for err == nil || le != nil {
		le, err = db.RedLogEntry(STR_TYPE, offest)
		if le != nil {
			t.Logf(le.ToString())
			offest += le.GetSize()
		}
	}
}
