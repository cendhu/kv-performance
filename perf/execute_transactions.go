package perf

import (
	"crypto/sha256"
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

	valueSizes := []int{0}
	batchLengths := []int{1, 10, 50, 100, 500, 1000, 2000, 4000, 6000, 8000, 10000}
	// batchLengths := []int{1, 10, 50, 100}

	var key []byte
	var keysSize int
	var totalSize float64

	h := sha256.New()

	for _, valueSize := range valueSizes {
		value := make([]byte, valueSize)
		for _, batchLength := range batchLengths {
			fmt.Println("")
			for i := batchNo; i < totalBatches; i++ {
				for j := 0; j < batchLength; j++ {
					key = []byte("v" + strconv.Itoa(valueSize) + "bl" + strconv.Itoa(batchLength) + "bn" + strconv.Itoa(i) + "b" + strconv.Itoa(j))
					h.Write(key)
					keyHash := h.Sum(nil)

					if valueSize != 0 {
						_, err := rand.Read(value)
						if err != nil {
							log.Fatal(err)
							return
						}
					} else {
						value = nil
					}

					keysSize += len(keyHash)

					if err := b.Put(keyHash, value); err != nil {
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

				totalSize += float64(keysSize) + float64(valueSize*batchLength)
				keysSize = 0
			}
		}
	}

	b.SetWrittenSize(uint64(totalSize))
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
