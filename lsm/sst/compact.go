package sst

import (
	"bytes"
	"container/heap"
	"os"

	"github.com/wubba-com/lsm-distributed/lsm/bloom"
	"github.com/wubba-com/lsm-distributed/lsm/encoder"
)

type iterator struct {
	it     *FileIterator
	seqNum uint64
}

func push(h *Heap, it *iterator) {
	if has := it.it.HasNext(); has {
		if k, v, err := it.it.Next(); err == nil {
			//log.Println("key", string(k))
			heap.Push(h, &Node{Seq: it.seqNum, SST: ElemSST{Key: k, Val: v}, It: it})
		}
	}
}

func pop(h *Heap) *Node {
	return heap.Pop(h).(*Node)
}

func Compact(dirname string, files []LevelFile, level Level, size uint32, sparseKeyDistance int32, removed bool) (SSTFile, error) {
	hp := &Heap{}
	heap.Init(hp)
	level += 1
	var (
		maxSeqNum uint64 = 0
		countKeys int
	)

	for idxFile := range files {
		binFile, idxFile, sparseFile, err := OpenBy(dirname, files[idxFile].Level, files[idxFile].SeqNum, os.O_RDONLY)
		if err != nil {
			return SSTFile{}, err
		}
		defer idxFile.Close()
		defer sparseFile.Close()

		it, err := NewIterator(binFile)
		if err != nil {
			if errClose := binFile.Close(); errClose != nil {
				return SSTFile{}, errClose
			}

			return SSTFile{}, err
		}
		defer it.CLose()

		c, err := readCountKeys(idxFile)
		if err != nil {
			return SSTFile{}, err
		}

		countKeys = c

		_, header, err := readSparseIndex(sparseFile)
		if err != nil {
			return SSTFile{}, err
		}

		if header.Seq > maxSeqNum {
			maxSeqNum = header.Seq
		}

		push(hp, &iterator{it: it, seqNum: header.Seq})
	}
	if hp.Len() == 0 {
		return SSTFile{}, nil
	}

	wr, err := NewWriter(dirname)
	if err != nil {
		return SSTFile{}, err
	}
	decoder := encoder.NewDecoder()
	filter := bloom.New(countKeys, 100)

	var write = func(key, val []byte, removed bool) error {
		if decoder.Decode(val).IsTombstone() && removed {
			return nil
		}
		filter.Add(string(key))

		if wr.Bytes() > int(size) {
			wr = NewWriter(dirname, level)
		}

		return wr.Write(key, val)
	}

	var (
		cur  = pop(hp)
		next *Node
	)
	push(hp, cur.It)

	for hp.Len() > 0 {
		next = pop(hp)
		push(hp, next.It)
		if cur != nil && bytes.Equal(cur.SST.Key, next.SST.Key) {
			if next.Seq > cur.Seq {
				cur = next
			}
			continue
		}
		if err := write(cur.SST.Key, cur.SST.Val, removed); err != nil {
			return SSTFile{}, err
		}

		cur = next
	}
	if err := write(cur.SST.Key, cur.SST.Val, removed); err != nil {
		return SSTFile{}, err
	}

	lvl := SSTFile{
		Level:  level,
		SeqNum: maxSeqNum,
		Filter: filter,
	}

	if err := wr.Close(); err != nil {
		return SSTFile{}, err
	}

	return lvl, nil
}
