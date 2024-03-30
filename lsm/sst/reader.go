package sst

import (
	"bytes"
	"fmt"
	"os"
)

const (
	minBytes = 1 << 2
	minSize  = 4 << 10
)

type Reader struct {
	binf    *os.File
	idxf    *os.File
	sparsef *os.File

	sprstat os.FileInfo

	buf          *bytes.Buffer
	sizeIdxBlock uint32
	lenKeys      uint32
	seqNum       uint32
}

func New(filepath string) (*Reader, error) {
	bin, idx, spr, err := OpenBy(filepath)
	if err != nil {
		return nil, err
	}

	stat, err := spr.Stat()
	if err != nil {
		return nil, err
	}

	r := &Reader{
		binf:    bin,
		idxf:    idx,
		sparsef: spr,
		sprstat: stat,
		buf:     bytes.NewBuffer(make([]byte, minSize)),
	}

	neededbytes := 3 * minBytes

	buf := r.buf.AvailableBuffer()[:neededbytes]

	pos := r.sprstat.Size() - int64(neededbytes)
	if _, err := r.sparsef.ReadAt(buf, pos); err != nil {
		return nil, err
	}

	r.lenKeys = decodeUInt32(buf[:minBytes])
	r.seqNum = decodeUInt32(buf[:minBytes])
	r.sizeIdxBlock = decodeUInt32(buf[:minBytes])

	offsets := r.buf.AvailableBuffer()
	start := int64(r.lenKeys * 16)
	end := int64(r.lenKeys*16) + int64(r.lenKeys*16) - int64(neededbytes)

	n, err := r.sparsef.ReadAt(offsets[start:end], start)
	if n < len(offsets[start:end]) {
		return nil, fmt.Errorf("read %d < n %d", len(offsets[start:end]), n)
	}

	return r, nil
}

func (r *Reader) search(key []byte) {
	low, high := 0, int(r.lenKeys)
	for low < high {
		mid := (low + high) / 2

	}
}
