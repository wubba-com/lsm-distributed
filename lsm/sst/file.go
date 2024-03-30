package sst

import (
	"fmt"
	"io"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"

	"github.com/wubba-com/lsm-distributed/lsm/bloom"
)

type Level uint16

const (
	BaseLevel Level = iota
)

type File struct {
	Level Level
	Num   uint64
	Name  string
	Ext   string
}

const (
	// DiskTable data file name. It contains raw data.
	extBin = ".bin"
	// DiskTable key file name. It contains keys and positions to values in the data file.
	extIdx = ".idx"
	// DiskTable sparse index. A sampling of every 64th entry in the index file.
	extSparse = ".spr"
	// A flag to open file for new disk table files: data, index and sparse index.
	newflags = os.O_WRONLY | os.O_CREATE | os.O_TRUNC | os.O_APPEND
)

type LevelFile struct {
	Level  Level
	SeqNum uint64
	Ext    string
}

func NewIterator(f *os.File) (*FileIterator, error) {
	key, val, err := Decode(f)
	if err != nil && err != io.EOF {
		return nil, err
	}

	return &FileIterator{
		fd:  f,
		err: err,
		key: key,
		val: val,
	}, nil
}

func NewFileIterator(filepath string) (*FileIterator, error) {
	fd, err := os.OpenFile(filepath, os.O_RDONLY, os.FileMode(0600))
	if err != nil {
		return nil, err
	}

	key, val, err := Decode(fd)
	if err != nil && err != io.EOF {
		return nil, err
	}

	return &FileIterator{
		fd:  fd,
		err: err,
		key: key,
		val: val,
	}, nil
}

type FileIterator struct {
	fd  *os.File
	key []byte
	val []byte
	err error
}

func (it *FileIterator) HasNext() bool {
	return it.err != io.EOF && it.err == nil
}

func (it *FileIterator) Next() ([]byte, []byte, error) {
	k, v := it.key, it.val

	nextKey, nextVal, err := Decode(it.fd)
	if err != nil && err != io.EOF {
		it.err = err
		return nil, nil, err
	}
	if err == io.EOF {
		it.err = io.EOF
	}

	it.key = nextKey
	it.val = nextVal

	return k, v, nil
}

func (it *FileIterator) CLose() error {
	return it.fd.Close()
}

// Levels возвращает имена любых каталогов, содержащих консолидированные
// Файлы SST на уровнях, превышающих уровень 0. Это означает, что данные
// организованы в неперекрывающиеся области между файлами на этом уровне.
func Levels(path string) ([]string, error) {
	files, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	var lvls []string
	for _, file := range files {
		matched, _ := regexp.Match(`^level-[0-9]*`, []byte(file.Name()))
		if matched && file.IsDir() {
			lvls = append(lvls, file.Name())
		}
	}

	return lvls, nil
}

func PathForLevel(base string, level int) string {
	return fmt.Sprintf("%s/level-%d", base, level)
}

// Filenames возвращает имена бинарных файлов SST по пути
func Filenames(path string) ([]string, error) {
	var sstFiles []string
	files, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		matched, _ := regexp.Match(`^sst-[0-9]*\.bin`, []byte(file.Name()))
		if matched && !file.IsDir() {
			sstFiles = append(sstFiles, file.Name())
		}
	}

	return sstFiles, nil
}

func NewSSTFiles(dirname string, bin string) (*os.File, *os.File, *os.File, error) {
	p := path.Join(dirname, bin)
	df, err := os.OpenFile(p, newflags, 0600)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to open file %s: %w", p, err)
	}

	p = path.Join(dirname, indexFileForBin(bin))
	idxf, err := os.OpenFile(p, newflags, 0600)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to open file %s: %w", p, err)
	}

	p = path.Join(dirname, sparseFileForBin(bin))
	sparseIdxf, err := os.OpenFile(p, newflags, 0600)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to open file %s: %w", p, err)
	}

	return df, idxf, sparseIdxf, nil
}

// NextFilename returns the name of the next SST binary file in given directory
func NextFilename(path string) (string, error) {
	files, err := os.ReadDir(path)
	if err != nil {
		return "", err
	}

	var sstFiles []string
	for _, file := range files {
		matched, _ := regexp.Match(`^sst-[0-9]*\.bin`, []byte(file.Name()))
		if matched && !file.IsDir() {
			sstFiles = append(sstFiles, file.Name())
		}
	}

	if len(sstFiles) > 0 {
		var latest = sstFiles[len(sstFiles)-1][4:8]
		n, _ := strconv.Atoi(latest)
		return fmt.Sprintf("sst-%04d.bin", n+1), nil
	}

	return "sst-0000.bin", nil
}

// Get filename of index file for given SST file
func sparseFileForIdx(filename string) string {
	return strings.TrimSuffix(filename, extIdx) + extSparse
}

// Get filename of index file for given SST file
func sparseFileForBin(filename string) string {
	return strings.TrimSuffix(filename, extBin) + extSparse
}

// Get filename of index file for given SST file
func indexFileForBin(filename string) string {
	return strings.TrimSuffix(filename, extBin) + extIdx
}

// Get filename of index file for given SST file
func indexFileForSparse(filename string) string {
	return strings.TrimSuffix(filename, extSparse) + extIdx
}

// Get filename of binary file for given index file
func binFileForIndex(filename string) string {
	return strings.TrimSuffix(filename, extIdx) + extBin
}

// Get filename of binary file for given index file
func binFileForSparse(filename string) string {
	return strings.TrimSuffix(filename, extSparse) + extBin
}

func OpenBy(binpath string) (*os.File, *os.File, *os.File, error) {
	var (
		err      error
		binFile  *os.File
		idxFile  *os.File
		sparFile *os.File
	)
	defer func() {
		if err != nil {
			if binFile != nil {
				binFile.Close()
			}
			if idxFile != nil {
				binFile.Close()
			}
			if sparFile != nil {
				binFile.Close()
			}
		}
	}()
	binFile, err = os.OpenFile(binpath, os.O_RDONLY, os.FileMode(0600))
	if err != nil {
		return nil, nil, nil, err
	}

	idxFile, err = os.OpenFile(indexFileForBin(binpath), os.O_RDONLY, os.FileMode(0600))
	if err != nil {
		return nil, nil, nil, err
	}

	sparFile, err = os.OpenFile(sparseFileForBin(binpath), os.O_RDONLY, os.FileMode(0600))
	if err != nil {
		return nil, nil, nil, err
	}

	return binFile, idxFile, sparFile, nil
}

func NewMemMetaSST(filename string, level Level, filter *bloom.Filter) (SSTFile, error) {
	_, h, err := readSparseIndexFile(filename)
	if err != nil {
		return SSTFile{}, err
	}

	return SSTFile{
		Filter: filter,
		SeqNum: h.Seq,
		Level:  level,
	}, nil
}
