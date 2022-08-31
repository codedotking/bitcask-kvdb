package db

import (
	"encoding/binary"
	"fmt"
)

const (
	OPT_ADD uint16 = iota
	OPT_DEL

	// 日志记录的请求头大小
	LOG_ENTRY_HEADER_SIZE = 10
)

// 一条日志记录的元数据
type logEntry struct {
	OptType   uint16
	KeySize   uint32
	ValueSize uint32
	Key       []byte
	Value     []byte
}

// NewLogEntry 创建一个日志记录
func NewLogEntry(key, value string, optType uint16) *logEntry {
	temp_key := []byte(key)
	temp_value := []byte(value)
	return &logEntry{
		Key:       temp_key,
		Value:     temp_value,
		KeySize:   uint32(len(temp_key)),
		ValueSize: uint32(len(temp_value)),
		OptType:   optType,
	}
}

// 获取当前日志记录信息的大小
func (e *logEntry) GetSize() int64 {
	return int64(LOG_ENTRY_HEADER_SIZE + e.KeySize + e.ValueSize)
}

// Encode 编码 Entry，返回字节数组
func (e *logEntry) Encode() ([]byte, error) {
	buf := make([]byte, e.GetSize())
	binary.BigEndian.PutUint16(buf[0:2], e.OptType)
	binary.BigEndian.PutUint32(buf[2:6], e.KeySize)
	binary.BigEndian.PutUint32(buf[6:10], e.ValueSize)
	copy(buf[LOG_ENTRY_HEADER_SIZE:LOG_ENTRY_HEADER_SIZE+e.KeySize], e.Key)
	copy(buf[LOG_ENTRY_HEADER_SIZE+e.KeySize:], e.Value)
	return buf, nil
}

// ToString 打印日志记录
func (e *logEntry) ToString() string {
	return fmt.Sprintf("optType = %d ,key = %s, value = %s", e.OptType, e.Key, e.Value)
}

// Decode 解码 buf 字节数组，返回 Entry
func Decode(buf []byte) (*logEntry, error) {
	optType := binary.BigEndian.Uint16(buf[0:2])
	ks := binary.BigEndian.Uint32(buf[2:6])
	vs := binary.BigEndian.Uint32(buf[6:10])
	key := buf[LOG_ENTRY_HEADER_SIZE : LOG_ENTRY_HEADER_SIZE+ks]
	value := buf[LOG_ENTRY_HEADER_SIZE+ks:]
	return &logEntry{KeySize: ks, ValueSize: vs, OptType: optType, Key: key, Value: value}, nil
}

// DecodeHeader 解码日志记录的头
func DecodeHeader(buf []byte) (*logEntry, error) {
	optType := binary.BigEndian.Uint16(buf[0:2])
	ks := binary.BigEndian.Uint32(buf[2:6])
	vs := binary.BigEndian.Uint32(buf[6:10])
	return &logEntry{KeySize: ks, ValueSize: vs, OptType: optType}, nil
}
