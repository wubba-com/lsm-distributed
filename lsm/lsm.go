package lsm

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"sync"
	"time"

	"github.com/wubba-com/lsm-distributed/lsm/bloom"
	"github.com/wubba-com/lsm-distributed/lsm/encoder"
	"github.com/wubba-com/lsm-distributed/lsm/memtable"
	"github.com/wubba-com/lsm-distributed/lsm/sst"
	"github.com/wubba-com/lsm-distributed/lsm/wal"
)

const (
	// MaxKeySize is the maximum allowed key size.
	// The size is hard-coded and must not be changed since it has
	// impact on the encoding features.
	MaxKeySize = math.MaxUint16
	// MaxValueSize is the maximum allowed value size.
	// The size is hard-coded and must not be changed since it has
	// impact on the encoding features.
	MaxValueSize = math.MaxUint16
)

const (
	// WAL file name.
	walFileName = "wal.db"
	// Default MemTable table threshold.
	defaultMemTableThreshold = 64000 // 64 kB
	// Default distance between keys in sparse index.
	defaultSparseKeyDistance = 128
	// Default DiskTable number threshold.
	defaultDiskTableNumThreshold = 10
)

var (
	// ErrKeyRequired is returned when putting a zero-length key or nil.
	ErrKeyRequired = errors.New("key required")
	// ErrValueRequired is returned when putting a zero-length value or nil.
	ErrValueRequired = errors.New("value required")
	// ErrKeyTooLarge is returned when putting a key that is larger than MaxKeySize.
	ErrKeyTooLarge = errors.New("key too large")
	// ErrValueTooLarge is returned when putting a value that is larger than MaxValueSize.
	ErrValueTooLarge = errors.New("value too large")
)

// LSMTree (https://en.wikipedia.org/wiki/Log-structured_merge-tree)
// это реализация лог-структуры merge-tree для хранения данных в файлах.
// Реализация не является goroutine-safe! Убедитесь, что при необходимости доступ
// к дереву синхронизируется.
type LSMTree struct {
	// Путь к каталогу, в котором хранятся файлы дерева LSM,
	// требуется указать выделенный каталог для каждого
	// экземпляра дерева.
	root string

	logger  *slog.Logger
	lock    sync.RWMutex
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	encoder *encoder.Encoder
	decoder *encoder.Decoder
	levels  []sst.SSTLevel
	debug   bool
	bufSize int
	config  *Config

	// Перед выполнением любой операции записи,
	// она записывается в журнал опережающей записи (WAL) и только потом применяется.
	wal  *wal.WAL
	cSST chan sst.ElemSST

	// Все изменения, которые стираются в WAL, но не стираются
	// в отсортированные файлы, хранятся в памяти для ускорения поиска.
	mem *memtable.Memtable

	// Если размер MemTable в байтах превышает пороговое значение, она должна быть
	// быть смыта в файловую систему.

	// Если число DiskTable превышает порог, дисковые таблицы должны быть
	// объединить, чтобы уменьшить его.
	diskTableNumThreshold int

	// Distance between keys in sparse index.
	sparseKeyDistance int32
}

func DebugMode(debug bool) func(*LSMTree) {
	return func(l *LSMTree) {
		l.debug = debug
	}
}

// MemTableThreshold устанавливает порог memTable для дерева LSM.
// Если размер MemTable в байтах превышает пороговое значение, он должен
// быть сброшен в файловую систему.
func MemTableThreshold(memTableThreshold uint32) func(*LSMTree) {
	return func(t *LSMTree) {
		t.config.MemtblDataSize = memTableThreshold
	}
}

// SparseKeyDistance устанавливает расстояние между разреженными ключами для дерева LSM.
// Расстояние между ключами в разреженном индексе.
func SparseKeyDistance(sparseKeyDistance int32) func(*LSMTree) {
	return func(t *LSMTree) {
		t.sparseKeyDistance = sparseKeyDistance
	}
}

// DiskTableNumThreshold устанавливает diskTableNumThreshold для дерева LSM.
// Если номер дисковой таблицы превышает пороговое значение, дисковые таблицы должны быть
// объединены, чтобы уменьшить его.
func DiskTableNumThreshold(diskTableNumThreshold int) func(*LSMTree) {
	return func(t *LSMTree) {
		t.diskTableNumThreshold = diskTableNumThreshold
	}
}

var logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
	Level: slog.LevelDebug,
}))

// Open открывает базу данных. Только одному экземпляру дерева разрешено
// читать и записывать в каталог.
func Open(path string, options ...func(*LSMTree)) (*LSMTree, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.MkdirAll(path, os.FileMode(0600))
	}

	wal, err := wal.NewWAL(path, wal.FileSync(false))
	if err != nil {
		return nil, err
	}

	memTable, err := wal.LoadMem()
	if err != nil {
		return nil, fmt.Errorf("failed to load mem from %s: %w", wal.Path(), err)
	}
	var sstLvls []sst.SSTLevel
	sstLvls = append(sstLvls, sst.SSTLevel{})

	// diskTableNum, maxDiskTableIndex, err := readDiskTableMeta(dbDir)
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to read disk table meta: %w", err)
	// }

	ctx, cancel := context.WithCancel(context.Background())

	t := &LSMTree{
		ctx:    ctx,
		cancel: cancel,
		wal:    wal,
		cSST:   make(chan sst.ElemSST),
		mem:    memTable,
		levels: sstLvls,
		root:   path,
		config: &Config{
			MemtblDataSize: defaultMemTableThreshold,
		},
		sparseKeyDistance:     defaultSparseKeyDistance,
		diskTableNumThreshold: defaultDiskTableNumThreshold,
		logger:                logger,
		encoder:               encoder.NewEncoder(),
		decoder:               encoder.NewDecoder(),
	}
	for _, option := range options {
		option(t)
	}
	t.wg.Add(1)
	go t.walJob()

	t.wg.Add(1)
	go t.mergeJob()

	return t, nil
}

type Config struct {
	MemtblDataSize uint32
	Merge          MergeSettings
}

// Define parameters for managing the SST levels
type MergeSettings struct {
	// Merge immediately from main thread if this is set to true
	Immediate bool

	// Maximum number of SST levels
	MaxLevels int

	// Amount of time to wait before checking to see if any levels need a merge
	Interval time.Duration

	// TODO: may be best if we have a job on its own thread checking on an interval (config here)
	// to see if the following conditions are true. If so initiate a merge.
	// that job could run some merges concurrently as long as there is no conflict. Maybe we do
	// that later as an enhancement

	// Compact if data in a level reaches this size
	DataSize uint32

	// Compact if a level contains more files than this
	NumberOfSstFiles int

	// Relocate data from level 0 after this time window (in seconds) is exceeded
	TimeWindow uint32
}

// Close closes all allocated resources.
func (t *LSMTree) Close() error {
	if err := t.wal.Close(); err != nil {
		return fmt.Errorf("failed to close file %s: %w", t.wal.Name(), err)
	}

	return nil
}

func (t *LSMTree) walJob() {
	defer t.wg.Done()
	for {
		select {
		case sst, ok := <-t.cSST:
			if !ok {
				return
			}

			if err := t.wal.Append(sst.Key, sst.Val); err != nil {
				logger.Error(err.Error())
			}
			if t.debug {
				logger.Debug(fmt.Sprintf("append wal: %s", string(sst.Key)))
			}

			if t.mem.Size() >= t.config.MemtblDataSize {
				if err := t.flushMemTable(); err != nil {
					logger.Error(err.Error())
				}
			}

		case <-t.ctx.Done():
			return
		}
	}
}

// Put puts the key into the db.
func (t *LSMTree) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyRequired
	} else if len(key) > MaxKeySize {
		return ErrKeyTooLarge
	} else if len(value) == 0 {
		return ErrValueRequired
	} else if uint64(len(value)) > MaxValueSize {
		return ErrValueTooLarge
	}

	value = t.encoder.Encode(encoder.OpKindSet, value)
	t.lock.Lock()
	t.cSST <- sst.ElemSST{Key: key, Val: value}
	t.mem.Put(key, value)
	t.lock.Unlock()
	return nil
}

// Get the value for the key from the db.
func (t *LSMTree) Get(key []byte) ([]byte, bool, error) {
	value, exists := t.mem.Get(key)
	if exists {
		if t.debug {
			logger.Debug("found key memtable")
		}
		dec := t.decoder.Decode(value)
		if dec.IsTombstone() {
			return dec.Value(), false, fmt.Errorf("key not found")
		}

		return dec.Value(), dec.Value() != nil, nil
	}

	value, exists, err := sst.SearchInDiskTables(key, t.root, t.levels)
	if err != nil {
		return nil, false, fmt.Errorf("failed to search in DiskTables: %w", err)
	}

	if exists {
		dec := t.decoder.Decode(value)

		if dec.IsTombstone() {
			return nil, false, fmt.Errorf("key not found")
		}

		if t.debug {
			logger.Debug("found key disk")
		}

		return dec.Value(), exists, nil
	}

	return nil, exists, fmt.Errorf("key not found")

}

// Delete delete the value by key from the db.
func (t *LSMTree) Delete(key []byte) error {
	val := t.encoder.Encode(encoder.OpKindDelete, nil)
	if err := t.wal.Append(key, val); err != nil {
		return fmt.Errorf("failed to append to file %s: %w", t.wal.Name(), err)
	}

	t.mem.Put(key, val)

	return nil
}

// flushMemTable сбрасывает текущую MemTable на диск и очищает ее.
// Функция ожидает, что она будет выполняться в синхронизированном блоке,
// и поэтому не использует никаких механизмов синхронизации.
func (t *LSMTree) flushMemTable() error {
	wr, err := sst.NewWriter(t.root, sst.BaseLevel, t.wal.Sequence(), int32(t.sparseKeyDistance))
	if err != nil {
		return err
	}
	mem := t.mem.Switch()

	filter := bloom.New(mem.Len(), 100)
	it := mem.Iterator()
	for it.HasNext() {
		k, v := it.Next()
		filter.Add(string(k))
		if err := wr.Write(k, v); err != nil {
			return err
		}
	}

	if err := wr.Close(); err != nil {
		return err
	}
	memMeta, err := sst.NewMemMetaSST(wr.NameSparseFile(), sst.BaseLevel, filter)
	if err != nil {
		return err
	}
	if err := t.wal.UpSequence(); err != nil {
		return err
	}

	t.levels[sst.BaseLevel].Files = append(t.levels[sst.BaseLevel].Files, memMeta)
	t.wal.Clear()

	if t.debug {
		t.logger.Debug("flush mem")
	}

	return nil
}

func (t *LSMTree) Shutdown() error {
	t.cancel()

	close(t.cSST)
	t.wg.Wait()

	return nil
}
