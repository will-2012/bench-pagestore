package pagestore

import (
	"math/rand"
	"sync"
	"time"
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
		dataSize := 3*1024 + delta*i
		bytes := make([]byte, dataSize)
		rand.Read(bytes)
		wGen.randDataPool[i] = bytes /*3k ~ 5k*/
	}
	wGen.currentVersion = 0
}

func (wGen *BenchWriteGenerator) Generate() (*PageID, *PageData) {
	if wGen == nil {
		return nil, nil
	}

	wGen.lock.Lock()
	defer wGen.lock.Unlock()

	wGen.currentVersion = wGen.currentVersion + 1
	index := rand.Intn(defaultRandDataNum)

	return &PageID{
		version: wGen.currentVersion,
		trieID:  Hash{},
		path:    nil,
	}, &PageData{rawData: wGen.randDataPool[index]}
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

func (rGen *BenchReadGenerator) Generate() *PageID {
	if rGen == nil {
		return nil
	}
	randomOffset := rand.Intn(int(rGen.endVersion - rGen.startVersion))
	return &PageID{
		version: rGen.startVersion + uint64(randomOffset),
		trieID:  Hash{},
		path:    nil,
	}
}
