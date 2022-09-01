package main

import (
	"fmt"
	"kv-performance/perf"
	"log"
	"os"

	"github.com/linxGnu/grocksdb"
)

func main() {
	os.RemoveAll("demo")

	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(grocksdb.NewLRUCache(3 << 30))

	opts := grocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)

	db, err := grocksdb.OpenDb(opts, "demo")
	if err != nil {
		log.Fatal(err)
	}

	wo := grocksdb.NewDefaultWriteOptions()
	wo.SetSync(true)

	ro := grocksdb.NewDefaultReadOptions()

	bc := &batchCommitter{
		db:    db,
		batch: grocksdb.NewWriteBatch(),
		wo:    wo,
		ro:    ro,
	}

	perf.Execute(bc)

	fmt.Printf("written size is %d and actual size is %f \n", bc.totalSize, perf.DirSizeMB("demo"))
}

type batchCommitter struct {
	db        *grocksdb.DB
	batch     *grocksdb.WriteBatch
	wo        *grocksdb.WriteOptions
	ro        *grocksdb.ReadOptions
	totalSize uint64
}

func (c *batchCommitter) Put(key, value []byte) error {
	c.batch.Put(key, value)
	return nil
}

func (c *batchCommitter) Commit() error {
	if err := c.db.Write(c.wo, c.batch); err != nil {
		return err
	}

	c.batch.Clear()
	return nil
}

func (c *batchCommitter) Get(key []byte) ([]byte, error) {
	r, err := c.db.Get(c.ro, key)
	return r.Data(), err
}

func (c *batchCommitter) SetWrittenSize(size uint64) {
	c.totalSize = size / (1024 * 1024)
}
