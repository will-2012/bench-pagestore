package utils

import (
	"math/rand"
	"sync"
	"time"

	"bench-pagestore/pagestore"
)

const (
	defaultRandDataNum = 50
)

// BenchWriteGenerator is used by single write thread to ensure PageID is monotonically increasing.
type BenchWriteGenerator struct {
	lock           sync.RWMutex
	currentVersion uint64
	randDataPool   [][]byte
}

func (wGen *BenchWriteGenerator) Init() {
	if wGen == nil {
		return
	}
	rand.Seed(time.Now().UnixNano())
	wGen.randDataPool = make([][]byte, defaultRandDataNum)
	delta := (60*1024 - 10*1024) / defaultRandDataNum
	for i := 0; i < defaultRandDataNum; i++ {
		dataSize := 10*1024 + delta*i
		bytes := make([]byte, dataSize)
		rand.Read(bytes)
		wGen.randDataPool[i] = bytes /*3k ~ 5k*/
	}
	wGen.currentVersion = 0
}

func (wGen *BenchWriteGenerator) Generate() (*pagestore.PageID, *pagestore.PageData) {
	if wGen == nil {
		return nil, nil
	}

	wGen.lock.Lock()
	defer wGen.lock.Unlock()

	wGen.currentVersion = wGen.currentVersion + 1
	index := rand.Intn(defaultRandDataNum)

	return &pagestore.PageID{
		Version: wGen.currentVersion,
		TrieID:  pagestore.Hash{},
		Path:    nil,
	}, &pagestore.PageData{RawData: wGen.randDataPool[index]}
}

// BenchReadGenerator is used by multi read thread.
type BenchReadGenerator struct {
	startVersion uint64
	endVersion   uint64
}

func (rGen *BenchReadGenerator) Init(start, end uint64) {
	rGen.startVersion = start
	rGen.endVersion = end
	rand.Seed(time.Now().UnixNano())
	return
}

func (rGen *BenchReadGenerator) Generate() *pagestore.PageID {
	if rGen == nil {
		return nil
	}
	randomOffset := rand.Intn(int(rGen.endVersion - rGen.startVersion))
	return &pagestore.PageID{
		Version: rGen.startVersion + uint64(randomOffset),
		TrieID:  pagestore.Hash{},
		Path:    nil,
	}
}
