package sst

import (
	"io"
	"os"
)

func readSparseHeader(r io.Reader) (Header, error) {
	var (
		seqNum uint64
		err    error
	)
	if seqNum, err = readUint64(r); err != nil {
		return Header{}, err
	}

	return Header{
		Seq: seqNum,
	}, nil
}

func readCountKeysFile(filepath string) (int, error) {
	f, err := os.OpenFile(filepath, os.O_RDONLY, os.FileMode(0600))
	if err != nil {
		return 0, err
	}
	defer f.Close()

	return readCountKeys(f)
}

func readCountKeys(r io.Reader) (int, error) {
	var count int

	var err error
	for {
		_, _, err = Decode(r)
		if err != nil && err != io.EOF {
			return 0, err
		}

		if err == io.EOF {
			return count, nil
		}
		count++

	}
}

func readSparseIndex(r io.Reader) ([]SSTIndex, Header, error) {
	var idxs []SSTIndex

	header, err := readSparseHeader(r)
	if err != nil {
		return nil, Header{}, err
	}

	for {
		k, v, err := Decode(r)
		if err != nil && err != io.EOF {
			return nil, Header{}, err
		}
		if err == io.EOF {
			return idxs, header, nil
		}

		offset := decodeUInt64(v)
		idxs = append(idxs, SSTIndex{
			Key:    k,
			Offset: int(offset),
		})
	}
}

func readSparseIndexFile(filepath string) ([]SSTIndex, Header, error) {
	f, err := os.Open(filepath)
	if err != nil {
		return nil, Header{}, err
	}
	defer f.Close()

	return readSparseIndex(f)
}
