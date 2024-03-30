package lsm

import (
	"bytes"
	"testing"
	"time"
)

func TestGetPut(t *testing.T) {
	var key = []byte("a")
	var val = []byte("a")

	l, err := Open("levels", MemTableThreshold(4), DebugMode(true))
	if err != nil {
		t.Fatal(err)
	}

	defer l.Shutdown()

	if err := l.Put(key[:], val[:]); err != nil {
		t.Fatal(err)
	}
	if v, ok, err := l.Get(key[:]); err != nil {
		t.Fatal(err)
	} else {
		if !ok {
			t.Fatal("key not found!")
		}

		if !bytes.Equal(val[:], v) {
			t.Fatalf("%s(l=%d)!=%s(l=%d)", val[:], len(val[:]), v, len(v))
		}

		key = []byte("b")
		val = []byte("b")

		if err := l.Put(key[:], val[:]); err != nil {
			t.Fatal(err)
		}

		key = []byte("c")
		val = []byte("c")
		if err := l.Put(key[:], val[:]); err != nil {
			t.Fatal(err)
		}

		key = []byte("d")
		val = []byte("d")
		if err := l.Put(key[:], val[:]); err != nil {
			t.Fatal(err)
		}

		key = []byte("e")
		val = []byte("e")
		if err := l.Put(key[:], val[:]); err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(3 * time.Second)

}
