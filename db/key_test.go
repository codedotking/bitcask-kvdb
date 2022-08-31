package db

import (
	"fmt"
	"testing"
)

func TestKey(t *testing.T) {
	node := NewNode()
	fmt.Println(node)
}

func TestPut(t *testing.T) {
	node := NewNode()
	value := Value{value: "你好", dataType: 0}
	node.Put("guanzhu", value)

	fmt.Println(node.Get("guanzhu").(Value).value)
	if node.Get("guanzhu") == nil {
		t.Errorf("获取出错关注")
	}
	fmt.Println(node)
}
