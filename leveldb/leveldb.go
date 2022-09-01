package main

import (
	"fmt"
	"kv-performance/perf"
	"log"
	"os"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func main() {
	os.RemoveAll("demo")
	db, err := leveldb.OpenFile("demo", &opt.Options{})
	if err != nil {
		log.Fatal(err)
	}

	bc := &batchCommitter{
		db:    db,
		batch: &leveldb.Batch{},
	}
	perf.Execute(bc)

	fmt.Printf("written size is %d and actual size is %f \n", bc.totalSize, perf.DirSizeMB("demo"))
}

type batchCommitter struct {
	db        *leveldb.DB
	batch     *leveldb.Batch
	totalSize uint64
}

func (c *batchCommitter) Put(key, value []byte) error {
	c.batch.Put(key, value)
	return nil
}

func (c *batchCommitter) Commit() error {
	if err := c.db.Write(c.batch, &opt.WriteOptions{Sync: true}); err != nil {
		return err
	}

	c.batch.Reset()
	return nil
}

func (c *batchCommitter) Get(key []byte) ([]byte, error) {
	return c.db.Get(key, &opt.ReadOptions{})
}

func (c *batchCommitter) SetWrittenSize(size uint64) {
	c.totalSize = size / (1024 * 1024)
}
