package pagestore

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"bench-pagestore/monitor"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

const (
	benchDir = "bench_workspace"
)

type PageID struct {
	Version uint64
	TrieID  Hash // trie owner's hash.
	Path    []byte
}

func (id *PageID) encode() []byte {
	if id == nil {
		return nil
	}
	enc := make([]byte, 8)
	binary.BigEndian.PutUint64(enc, id.Version)
	return append(append(enc, id.TrieID[:]...), id.Path...)
}

type PageData struct {
	RawData []byte
}

func (data *PageData) encode() []byte {
	if data == nil {
		return nil
	}
	return data.RawData
}

type PageStore struct {
	db *leveldb.DB
}

// option v1 only has level0.
func getDBOptsV1() *opt.Options {
	dbOpts := &opt.Options{}

	// 1.It should be smaller than the disk bandwidth to avoid IO jitter caused by flushing memtable.
	// 2.Avoid too many keys causing filterblock to be too large, which affects read performance.
	dbOpts.WriteBuffer = 256 * 1024 * 1024 // 256MiB

	// Is a relatively large value, full memory cache all file handles.
	dbOpts.OpenFilesCacheCapacity = 81920 // db-size = 81920 * 256MiB = 20TiB

	// Adjust according to the actual memory of the physical machine.
	dbOpts.BlockCacheCapacity = 2 * 1024 * 1024 * 1024 // 2GiB

	// 10bits bloomfilter is a good tradeoff.
	dbOpts.Filter = filter.NewBloomFilter(10)

	// avg_kv_size = 35KiB, one block should include tens of kv.
	dbOpts.BlockSize = 128 * 1024 // 128KiB

	// one block should include several restart point.
	dbOpts.BlockRestartInterval = 4

	// the leveldb only has l0 level and no major compact.
	dbOpts.CompactionL0Trigger = math.MaxInt
	dbOpts.WriteL0SlowdownTrigger = math.MaxInt
	dbOpts.WriteL0PauseTrigger = math.MaxInt

	return dbOpts
}

// option v2 only has level0 and level1.
// speedup read, due to search l0 is slow, and l1 is faster(binary search).
func getDBOptsV2() *opt.Options {
	dbOpts := &opt.Options{}

	// 1.It should be smaller than the disk bandwidth to avoid IO jitter caused by flushing memtable.
	// 2.Avoid too many keys causing filterblock to be too large, which affects read performance.
	dbOpts.WriteBuffer = 256 * 1024 * 1024 // 256MiB

	// Is a relatively large value, full memory cache all file handles.
	dbOpts.OpenFilesCacheCapacity = 81920 // db-size = 81920 * 256MiB = 20TiB

	// Adjust according to the actual memory of the physical machine.
	dbOpts.BlockCacheCapacity = 2 * 1024 * 1024 * 1024 // 2GiB

	// 10bits bloomfilter is a good tradeoff.
	dbOpts.Filter = filter.NewBloomFilter(10)

	// avg_kv_size = 35KiB, one block should include tens of kv.
	dbOpts.BlockSize = 128 * 1024 // 128KiB

	// one block should include several restart point.
	dbOpts.BlockRestartInterval = 4

	// the leveldb only has l0/l1 level and l0-to-l2 is just move.
	dbOpts.WriteL0SlowdownTrigger = math.MaxInt
	dbOpts.WriteL0PauseTrigger = math.MaxInt
	dbOpts.CompactionTotalSize = math.MaxInt // ensure l1 score < 1 and avoid compact to high level.

	return dbOpts
}

// option v3 only has level0 and level1, reduce memtable size to 64MiB.
// speedup read/write mix, reduce the side effects of flush l0.
// It should be noted that this has a negative impact on the performance of reading recently written keys;
// therefore, cache should be added at the upper layer.
func getDBOptsV3() *opt.Options {
	dbOpts := &opt.Options{}

	// 1.It should be smaller than the disk bandwidth to avoid IO jitter caused by flushing memtable.
	// 2.Avoid too many keys causing filterblock to be too large, which affects read performance.
	dbOpts.WriteBuffer = 64 * 1024 * 1024 // 64MiB

	// Is a relatively large value, full memory cache all file handles.
	dbOpts.OpenFilesCacheCapacity = 81920 // db-size = 81920 * 64MiB = 5TiB

	// Adjust according to the actual memory of the physical machine.
	dbOpts.BlockCacheCapacity = 2 * 1024 * 1024 * 1024 // 2GiB

	// 10bits bloomfilter is a good tradeoff.
	dbOpts.Filter = filter.NewBloomFilter(10)

	// avg_kv_size = 35KiB, one block should include tens of kv.
	dbOpts.BlockSize = 128 * 1024 // 128KiB

	// one block should include several restart point.
	dbOpts.BlockRestartInterval = 4

	// the leveldb only has l0/l1 level and l0-to-l2 is just move.
	dbOpts.WriteL0SlowdownTrigger = math.MaxInt
	dbOpts.WriteL0PauseTrigger = math.MaxInt
	dbOpts.CompactionTotalSize = math.MaxInt // ensure l1 score < 1 and avoid compact to high level.

	return dbOpts
}

func Open() (*PageStore, error) {
	dbOpts := getDBOptsV3()

	db, err := leveldb.OpenFile(benchDir, dbOpts)
	if _, corrupted := err.(*errors.ErrCorrupted); corrupted {
		db, err = leveldb.RecoverFile(benchDir, nil)
	}
	if err != nil {
		return nil, err
	}

	ps := &PageStore{}
	ps.db = db

	return ps, nil
}

func (ps *PageStore) Close() error {
	if ps == nil {
		return nil
	}
	if ps.db == nil {
		return nil
	}
	return ps.db.Close()
}

func (ps *PageStore) Put(id *PageID, page *PageData) error {
	if ps == nil {
		return fmt.Errorf("failed to put page due to page store is nil")
	}
	if ps.db == nil {
		return fmt.Errorf("failed to put page due to page store db is nil")
	}
	start := time.Now()
	defer func() {
		monitor.RecordWriteDuration(time.Now().Sub(start))
	}()
	return ps.db.Put(id.encode(), page.encode(), nil)
}

func (ps *PageStore) Get(id *PageID) (*PageData, error) {
	if ps == nil {
		return nil, fmt.Errorf("failed to get page due to page store is nil")
	}
	if ps.db == nil {
		return nil, fmt.Errorf("failed to get page due to page store db is nil")
	}
	start := time.Now()
	defer func() {
		monitor.RecordReadDuration(time.Now().Sub(start))
	}()
	rawData, err := ps.db.Get(id.encode(), nil)
	return &PageData{RawData: rawData}, err
}
