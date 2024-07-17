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

func Open() (*PageStore, error) {
	dbOpts := &opt.Options{}

	// 1.It should be smaller than the disk bandwidth to avoid IO jitter caused by flushing memtable.
	// 2.Avoid too many keys causing filterblock to be too large, which affects read performance.
	dbOpts.WriteBuffer = 512 * 1024 * 1024 // 512MiB

	// Is a relatively large value, full memory cache all file handles.
	dbOpts.OpenFilesCacheCapacity = 81920 // db-size = 81920 * 512MiB = 4TiB

	// Adjust according to the actual memory of the physical machine.
	dbOpts.BlockCacheCapacity = 2 * 1024 * 1024 * 1024 // 2GiB

	// 10bits bloomfilter is a good tradeoff.
	dbOpts.Filter = filter.NewBloomFilter(10)

	// avg_kv_size = 35KiB, one block should include tens of kv.
	dbOpts.BlockSize = 512 * 1024 // 512KiB

	// one block should include several restart point.
	dbOpts.BlockRestartInterval = 2

	// the leveldb only has l0 level.
	dbOpts.CompactionL0Trigger = math.MaxInt
	dbOpts.WriteL0SlowdownTrigger = math.MaxInt
	dbOpts.WriteL0PauseTrigger = math.MaxInt

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
