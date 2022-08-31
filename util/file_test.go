package util

import (
	"testing"
)

func TestCreateFile(t *testing.T) {
	file, err := CreateFile("d:\\temp\\db.txt")
	if err != nil {
		t.Errorf("文件创建失败 %s", err)
		return
	}
	defer file.Close()
	if !PathExist("d:\\temp\\db.txt") {
		t.Errorf("文件创建失败")
	} else {
		t.Logf("文件创建成功")
	}
}
