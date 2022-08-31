package util

import (
	"testing"
)

func TestRandomString(t *testing.T) {
	rs := RandomEnString(5)
	if len(rs) != 5 {
		t.Errorf("生成长度错误")
	}
}
