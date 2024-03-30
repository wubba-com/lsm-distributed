package lsm

import (
	"crypto/rand"
	"testing"
	"time"
)

func BenchmarkMerge(b *testing.B) {
	l, err := Open("levels", MemTableThreshold(64<<10), DebugMode(false))
	if err != nil {
		panic(err)
	}
	defer l.Shutdown()

	l.SetMergeSettings(MergeSettings{
		Interval:         5 * time.Second,
		NumberOfSstFiles: 3,
		MaxLevels:        3,
	})

	for i := 0; i < b.N; i++ {
		var key [16]byte
		var val [16]byte

		rand.Read(key[:])
		rand.Read(val[:])

		if err := l.Put(key[:], val[:]); err != nil {
			panic(err)
		}
	}
}

func TestMerge(t *testing.T) {

	l, err := Open("levels", MemTableThreshold(4), DebugMode(false))
	if err != nil {
		t.Fatal(err)
	}
	l.SetMergeSettings(MergeSettings{
		Interval:         3 * time.Second,
		NumberOfSstFiles: 2,
		MaxLevels:        3,
	})

	defer l.Shutdown()

	l.Put([]byte("a"), []byte("a"))
	l.Put([]byte("b"), []byte("b"))

	l.logger.Debug("flush")
	l.Put([]byte("c"), []byte("c"))
	l.Put([]byte("d"), []byte("d"))

	time.Sleep(3 * time.Second)

	l.logger.Debug("flush")
	l.Put([]byte("e"), []byte("e"))
	l.Put([]byte("f"), []byte("f"))

	l.Put([]byte("g"), []byte("g"))
	l.Put([]byte("h"), []byte("h"))

	l.Put([]byte("g"), []byte("g"))
	l.Put([]byte("h"), []byte("h"))

	l.Put([]byte("i"), []byte("i"))
	l.Put([]byte("j"), []byte("j"))

	l.Put([]byte("k"), []byte("k"))
	l.Put([]byte("l"), []byte("l"))

	l.Put([]byte("m"), []byte("m"))
	l.Put([]byte("n"), []byte("n"))

	l.Put([]byte("o"), []byte("o"))
	l.Put([]byte("p"), []byte("p"))

	l.Put([]byte("a"), []byte("aa"))

	time.Sleep(20 * time.Second)

}
