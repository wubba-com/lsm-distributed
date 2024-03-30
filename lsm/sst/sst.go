package sst

import (
	"github.com/wubba-com/lsm-distributed/lsm/bloom"
)

type Header struct {
	Seq uint64
}

type SSTIndex struct {
	Key    []byte
	Offset int
}

type SSTLevel struct {
	Files []SSTFile
}

type SSTFile struct {
	Filter *bloom.Filter
	Level  Level
	SeqNum uint64
}

type ElemSST struct {
	Key, Val []byte
}
