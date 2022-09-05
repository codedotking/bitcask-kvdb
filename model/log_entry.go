package model

import (
	"encoding/binary"
	"github.com/he-wen-yao/bitcask-kvdb/constant"
	"hash/crc32"
)

type (
	// Entry 写入日志文件的操作记录实体
	LogEntry struct {
		Key   []byte
		Value []byte
		crc   uint32
		Meta  *MetaData
	}

	// Hint 内存中 Key 的映射
	Hint struct {
		Value  []byte
		FileID int64
		Meta   *MetaData
	}

	// MetaData 日志记录的元数据
	MetaData struct {
		Timestamp uint64
		TTL       uint32
		Position  uint64
		KeySize   uint32
		ValueSize uint32
		OptType   uint16 //  表示日志记录的操作类型: SET / DELETE
	}
)

func (e *LogEntry) Size() uint64 {
	return uint64(constant.ENTRY_HEADER_SIZE + e.Meta.ValueSize + e.Meta.KeySize)
}

func (e *LogEntry) Encode() []byte {
	keySize := e.Meta.KeySize
	valueSize := e.Meta.ValueSize
	// 初始化细节切片
	buf := make([]byte, e.Size())
	buf = e.setEntryHeaderBuf(buf)
	// set bucket\key\value
	headerSize := constant.ENTRY_HEADER_SIZE
	copy(buf[headerSize:headerSize+keySize], e.Key)
	copy(buf[headerSize+keySize:headerSize+keySize+valueSize], e.Value)
	// 获取已经写入 buf 的数据获取校验和
	c32 := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], c32)
	return buf
}

func (e *LogEntry) setEntryHeaderBuf(buf []byte) []byte {
	binary.LittleEndian.PutUint64(buf[4:12], e.Meta.Timestamp)
	binary.LittleEndian.PutUint32(buf[12:16], e.Meta.KeySize)
	binary.LittleEndian.PutUint32(buf[16:20], e.Meta.ValueSize)
	binary.LittleEndian.PutUint16(buf[20:22], e.Meta.OptType)
	binary.LittleEndian.PutUint32(buf[22:26], e.Meta.TTL)
	binary.LittleEndian.PutUint64(buf[30:34], e.Meta.Position)
	return buf
}

// GetCrc 重新计算 Entry 的 crc 的校验值
func (e *LogEntry) GetCrc(buf []byte) uint32 {
	crc := crc32.ChecksumIEEE(buf[4:])
	crc = crc32.Update(crc, crc32.IEEETable, e.Key)
	crc = crc32.Update(crc, crc32.IEEETable, e.Value)
	return crc
}
