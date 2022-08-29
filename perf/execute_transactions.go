package perf

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

type Batch interface {
	Put(key []byte, value []byte) error
	Commit() error
	SetWrittenSize(size uint64)
}

func Execute(b Batch) {
	batchNo := 1
	totalBatches := 10

	valueSizes := []int{32, 64, 128, 512, 1024, 2048, 4096}
	batchLengths := []int{1, 10, 50, 100, 500, 1000, 2000, 4000, 6000, 8000, 10000}
	// batchLengths := []int{1, 10, 50, 100}

	var key []byte
	var keysSize int
	var totalSize uint64

	for _, valueSize := range valueSizes {
		value := make([]byte, valueSize)
		for _, batchLength := range batchLengths {
			fmt.Println("")
			for i := batchNo; i < totalBatches; i++ {
				for j := 0; j < batchLength; j++ {
					key = []byte("v" + strconv.Itoa(valueSize) + "bl" + strconv.Itoa(batchLength) + "bn" + strconv.Itoa(i) + "b" + strconv.Itoa(j))
					_, err := rand.Read(value)
					if err != nil {
						log.Fatal(err)
						return
					}

					keysSize += len(key)

					if err = b.Put(key, value); err != nil {
						log.Fatal(err)
						return
					}
				}

				start := time.Now()
				if err := b.Commit(); err != nil {
					log.Fatal(err)
					return
				}
				log.Printf("Time taken to commit %d #KV pairs with valueSize %d and average key size %d bytes is %d", batchLength, valueSize, keysSize/batchLength, time.Since(start).Milliseconds())

				totalSize += uint64(keysSize) + uint64(valueSize*batchLength)
				keysSize = 0
			}
		}
	}

	b.SetWrittenSize(totalSize)
}

func DirSizeMB(path string) float64 {
	sizes := make(chan int64)
	readSize := func(path string, file os.FileInfo, err error) error {
		if err != nil || file == nil {
			return nil // Ignore errors
		}
		if !file.IsDir() {
			sizes <- file.Size()
		}
		return nil
	}

	go func() {
		filepath.Walk(path, readSize)
		close(sizes)
	}()

	size := int64(0)
	for s := range sizes {
		size += s
	}

	sizeMB := float64(size) / 1024.0 / 1024.0

	return sizeMB
}
