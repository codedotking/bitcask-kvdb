// Package db string 类型命令实现
package db

func (db *bitCaskDB) Set(key, value string) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	lf, entrySize, err := db.AppendLog(key, value, STR_TYPE, OPT_ADD)
	if err != nil {
		return err
	}
	// 更新内存索引
	_, _ = db.strIndex.tree.Put(key, Value{offset: lf.offset, entrySize: entrySize})
	return nil
}

func (db *bitCaskDB) Get(key string) (string, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	v := db.strIndex.tree.Get(key)
	_, ok := v.(Value)
	if ok == false {
		return "", nil
	}
	// TODO 去实际文件根据 offset 位置读取 key 对应的 Value
	// 更新内存索引
	return "", nil
}
