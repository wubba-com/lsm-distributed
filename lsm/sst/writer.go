package sst

import (
	"bufio"
	"fmt"
	"os"
)

type OptionWriter func(w *Writer)

func SparseKeyDistance(sparseKeyDistance int32) OptionWriter {
	return func(w *Writer) {
		w.sparseKeyDistance = sparseKeyDistance
	}
}

func NewWriter(dirname string, options ...OptionWriter) (*Writer, error) {
	binName, err := NextFilename(dirname)
	if err != nil {
		return nil, err
	}

	bin, idx, spr, err := NewSSTFiles(dirname, binName)
	if err != nil {
		return nil, err
	}

	w := &Writer{
		fd:         bin,
		fidx:       idx,
		fsparseIdx: spr,

		bd:         bufio.NewWriter(bin),
		bidx:       bufio.NewWriter(idx),
		bsparseIdx: bufio.NewWriter(spr),
		keyNum:     0,
		dataPos:    0,
		indexPos:   0,
		n:          0,
	}

	for _, opt := range options {
		opt(w)
	}

	return w, nil
}

type Writer struct {
	fd         *os.File
	fidx       *os.File
	fsparseIdx *os.File

	bd         *bufio.Writer
	bidx       *bufio.Writer
	bsparseIdx *bufio.Writer

	idxB bool

	offsets                   []int32
	sparseKeyDistance         int32
	keyNum                    int32
	dataPos, indexPos, sprPos int
	n                         int
}

func (w *Writer) AddIdxBlock(seqNum uint64) error {
	var (
		err error
		n   int
	)
	for idx, off := range w.offsets {
		if n, err = WriteUInt32Pair(w.bsparseIdx, uint32(idx), uint32(off)); err != nil {
			return err
		}
		w.sprPos += n
	}
	
	if n, err = writeUint32(w.bsparseIdx, uint32(len(w.offsets))); err != nil {
		return err
	}
	w.sprPos += n

	if n, err = writeUint32(w.bsparseIdx, uint32(seqNum)); err != nil {
		return err
	}
	w.sprPos += n


	if n, err = writeUint32(w.bsparseIdx, uint32(w.sprPos)); err != nil {
		return err
	}
	w.sprPos += n
	w.idxB = true

	return nil
}

func (w *Writer) Write(key, val []byte) error {

	dBytes, err := Encode(w.bd, key, val)
	if err != nil {
		return fmt.Errorf("failed to write to the data file: %w", err)
	}

	idxBytes, err := EncodeKeyOffset(w.bidx, key, int(w.dataPos))
	if err != nil {
		return fmt.Errorf("failed to write to the index file: %w", err)
	}

	var sprBytes = 0
	if w.keyNum%w.sparseKeyDistance == 0 {
		if sprBytes, err = EncodeKeyOffset(w.bsparseIdx, key, int(w.indexPos)); err != nil {
			return fmt.Errorf("failed to write to the file: %w", err)
		}

		w.offsets = append(w.offsets, int32(w.sprPos))
		w.sprPos += sprBytes
	}

	w.dataPos += dBytes
	w.indexPos += idxBytes
	w.keyNum++
	w.n += len(key) + len(val)

	return nil
}

func (w *Writer) Bytes() int {
	return w.n
}

func (w *Writer) Len() int {
	return int(w.keyNum)
}

func (w *Writer) NameSparseFile() string {
	return w.fsparseIdx.Name()
}

func (w *Writer) NameIndexFile() string {
	return w.fidx.Name()
}

func (w *Writer) NameDataFile() string {
	return w.fd.Name()
}

func (w *Writer) Close() error {
	if !w.idxB {
		return fmt.Errorf("not added idx block")
	}

	if err := w.bd.Flush(); err != nil {
		return fmt.Errorf("err flush at the close: %s", err)
	}
	if err := w.bidx.Flush(); err != nil {
		return fmt.Errorf("err flush at the close: %s", err)
	}
	if err := w.bsparseIdx.Flush(); err != nil {
		return fmt.Errorf("err flush at the close: %s", err)
	}
	if err := w.fd.Sync(); err != nil {
		return fmt.Errorf("err sync at the close: %s", err)
	}
	if err := w.fidx.Sync(); err != nil {
		return fmt.Errorf("err sync at the close: %s", err)
	}
	if err := w.fsparseIdx.Sync(); err != nil {
		return fmt.Errorf("err sync at the close: %s", err)
	}
	if err := w.fd.Close(); err != nil {
		return fmt.Errorf("err close at the close: %s", err)
	}
	if err := w.fidx.Close(); err != nil {
		return fmt.Errorf("err close at the close: %s", err)
	}
	if err := w.fsparseIdx.Close(); err != nil {
		return fmt.Errorf("err close at the close: %s", err)
	}

	return nil
}
