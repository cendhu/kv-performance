package main

import (
	"fmt"
	"kv-performance/perf"
	"log"
	"os"

	"github.com/cockroachdb/pebble"
)

func main() {
	os.RemoveAll("demo")
	db, err := pebble.Open("demo", &pebble.Options{})
	if err != nil {
		log.Fatal(err)
	}

	bc := &batchCommitter{
		batch: db.NewBatch(),
		db:    db,
	}

	perf.Execute(bc)

	fmt.Printf("written size is %d and actual size is %f \n", bc.totalSize, perf.DirSizeMB("demo"))
}

type batchCommitter struct {
	batch     *pebble.Batch
	db        *pebble.DB
	totalSize uint64
}

func (c *batchCommitter) Put(key, value []byte) error {
	return c.batch.Set(key, value, nil)
}

func (c *batchCommitter) Commit() error {
	if err := c.batch.Commit(pebble.Sync); err != nil {
		return err
	}

	c.batch.Reset()
	return nil
}

func (c *batchCommitter) Get(key []byte) ([]byte, error) {
	v, _, err := c.db.Get(key)
	return v, err
}

func (c *batchCommitter) SetWrittenSize(size uint64) {
	c.totalSize = size / (1024 * 1024)
}
