package sst

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
)

// searchInDiskTables searches a value by the key in DiskTables, by traversing
// all tables in the directory.
func SearchInDiskTables(key []byte, dirname string, lvls []SSTLevel) ([]byte, bool, error) {
	for lvl := 0; lvl < len(lvls); lvl++ {
		for last := len(lvls[lvl].Files) - 1; last >= 0; last-- {
			seq := lvls[lvl].Files[last].SeqNum
			value, exists, err := searchInDiskTable(key, dirname, Level(lvl), seq)
			if err != nil {
				return nil, false, fmt.Errorf("failed to search in disk table with index %d lvl %d: %w", last, lvl, err)
			}

			if exists {
				return value, exists, nil
			}
		}
	}

	return nil, false, nil
}

// searchInDiskTable searches a given key in a given disk table.
func searchInDiskTable(key []byte, dirname string, lvl Level, seqNum uint64) ([]byte, bool, error) {
	df, idxf, spf, err := OpenBy(dirname, lvl, seqNum, os.O_RDONLY)
	if err != nil {
		return nil, false, err
	}
	defer func() {
		df.Close()
		idxf.Close()
		spf.Close()
	}()

	from, to, ok, err := searchInSparseIndex(spf, key)
	if err != nil {
		return nil, false, fmt.Errorf("failed to search in sparse index file %s: %w", path.Join(dirname, spf.Name()), err)
	}
	if !ok {
		return nil, false, nil
	}

	offset, ok, err := searchInIndex(idxf, from, to, key)
	if err != nil {
		return nil, false, fmt.Errorf("failed to search in index file %s: %w", path.Join(dirname, idxf.Name()), err)
	}
	if !ok {
		return nil, false, nil
	}

	value, ok, err := searchInDataFile(df, offset, key)
	if err != nil {
		return nil, false, fmt.Errorf("failed to search in data file %s: %w", path.Join(dirname, df.Name()), err)
	}

	return value, ok, nil
}

// searchInDataFile searches a value by the key in the data file from the given offset.
// The offset must always point to the beginning of the record.
func searchInDataFile(r io.ReadSeeker, offset int, searchKey []byte) ([]byte, bool, error) {
	if _, err := r.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, false, fmt.Errorf("failed to seek: %w", err)
	}

	for {
		key, value, err := Decode(r)
		if err != nil && err != io.EOF {
			return nil, false, fmt.Errorf("failed to read: %w", err)
		}
		if err == io.EOF {
			return nil, false, nil
		}

		if bytes.Equal(key, searchKey) {
			return value, true, nil
		}
	}
}

// searchInIndex searches key in the index file in specified range.
func searchInIndex(r io.ReadSeeker, from, to int, searchKey []byte) (int, bool, error) {
	if _, err := r.Seek(int64(from), io.SeekStart); err != nil {
		return 0, false, fmt.Errorf("failed to seek: %w", err)
	}

	for {
		key, value, err := Decode(r)
		if err != nil && err != io.EOF {
			return 0, false, fmt.Errorf("failed to read: %w", err)
		}
		if err == io.EOF {
			return 0, false, nil
		}
		offset := int(decodeUInt64(value))

		if bytes.Equal(key, searchKey) {
			return offset, true, nil
		}

		if to > from {
			current, err := r.Seek(0, io.SeekCurrent)
			if err != nil {
				return 0, false, fmt.Errorf("failed to seek: %w", err)
			}

			if current > int64(to) {
				return 0, false, nil
			}
		}
	}
}

// searchInSparseIndex searches a range between which the key is located.
func searchInSparseIndex(r io.Reader, searchKey []byte) (int, int, bool, error) {
	from := -1

	_, err := readSparseHeader(r)
	if err != nil {
		return from, 0, false, err
	}
	//log.Println("searchInSparseIndex seq num:", h.Seq)
	for {
		key, value, err := Decode(r)
		if err != nil && err != io.EOF {
			return 0, 0, false, fmt.Errorf("failed to read: %w", err)
		}
		if err == io.EOF {
			return from, 0, from != -1, nil
		}
		offset := int(decodeUInt64(value))

		cmp := bytes.Compare(key, searchKey)
		if cmp == 0 {
			return offset, offset, true, nil
		} else if cmp < 0 {
			from = offset
		} else if cmp > 0 {
			if from == -1 {
				// если первый ключ в разреженном индексе больше, чем
				// ключа поиска, это означает, что ключ отсутствует
				return 0, 0, false, nil
			} else {
				return from, offset, true, nil
			}
		}
	}
}
