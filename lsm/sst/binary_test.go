package sst

import (
	"bufio"
	"crypto/rand"
	"io"
	"log"
	"os"
	"slices"
	"testing"
)

func BenchmarkEncodeBufio2KB(b *testing.B) {
	nameFile := "bufio2.sst"
	f, err := os.OpenFile(nameFile, os.O_CREATE|os.O_RDWR|os.O_APPEND, os.FileMode(0600))
	if err != nil {
		panic(err)
	}

	var key [1 << 10]byte
	var val [1 << 10]byte
	if _, err := rand.Read(key[:]); err != nil {
		panic(err)
	}
	if _, err := rand.Read(val[:]); err != nil {
		panic(err)
	}

	w := bufio.NewWriter(f)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Encode(w, key[:], val[:]); err != nil {
			panic(err)
		}

	}
	w.Flush()
}

func BenchmarkEncodeFile2KB(b *testing.B) {
	nameFile := "file2.sst"
	f, err := os.OpenFile(nameFile, os.O_CREATE|os.O_RDWR|os.O_APPEND, os.FileMode(0600))
	if err != nil {
		panic(err)
	}

	var key [1 << 10]byte
	var val [1 << 10]byte
	if _, err := rand.Read(key[:]); err != nil {
		panic(err)
	}
	if _, err := rand.Read(val[:]); err != nil {
		panic(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Encode(f, key[:], val[:]); err != nil {
			panic(err)
		}
	}
}

func BenchmarkEncodeBufio4KB(b *testing.B) {
	nameFile := "bufio4.sst"
	f, err := os.OpenFile(nameFile, os.O_CREATE|os.O_RDWR|os.O_APPEND, os.FileMode(0600))
	if err != nil {
		panic(err)
	}

	var key [2 << 10]byte
	var val [2 << 10]byte
	if _, err := rand.Read(key[:]); err != nil {
		panic(err)
	}
	if _, err := rand.Read(val[:]); err != nil {
		panic(err)
	}

	w := bufio.NewWriter(f)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Encode(w, key[:], val[:]); err != nil {
			panic(err)
		}
	}
	w.Flush()
}

func BenchmarkEncodeFile4KB(b *testing.B) {
	nameFile := "file4.sst"
	f, err := os.OpenFile(nameFile, os.O_CREATE|os.O_RDWR|os.O_APPEND, os.FileMode(0600))
	if err != nil {
		panic(err)
	}

	var key [2 << 10]byte
	var val [2 << 10]byte
	if _, err := rand.Read(key[:]); err != nil {
		panic(err)
	}
	if _, err := rand.Read(val[:]); err != nil {
		panic(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Encode(f, key[:], val[:]); err != nil {
			panic(err)
		}
	}
}

func BenchmarkEncodeBufio8KB(b *testing.B) {
	nameFile := "bufio8.sst"
	f, err := os.OpenFile(nameFile, os.O_CREATE|os.O_RDWR|os.O_APPEND, os.FileMode(0600))
	if err != nil {
		panic(err)
	}

	var key [4 << 10]byte
	var val [4 << 10]byte
	if _, err := rand.Read(key[:]); err != nil {
		panic(err)
	}
	if _, err := rand.Read(val[:]); err != nil {
		panic(err)
	}

	w := bufio.NewWriter(f)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Encode(w, key[:], val[:]); err != nil {
			panic(err)
		}
	}
	w.Flush()

}

func BenchmarkEncodeFile8KB(b *testing.B) {
	nameFile := "file8.sst"
	f, err := os.OpenFile(nameFile, os.O_CREATE|os.O_RDWR|os.O_APPEND, os.FileMode(0600))
	if err != nil {
		panic(err)
	}

	var key [4 << 10]byte
	var val [4 << 10]byte
	if _, err := rand.Read(key[:]); err != nil {
		panic(err)
	}
	if _, err := rand.Read(val[:]); err != nil {
		panic(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Encode(f, key[:], val[:]); err != nil {
			panic(err)
		}
	}
}

func BenchmarkEncodeBufio16KB(b *testing.B) {
	nameFile := "bufio16.sst"
	f, err := os.OpenFile(nameFile, os.O_CREATE|os.O_RDWR|os.O_APPEND, os.FileMode(0600))
	if err != nil {
		panic(err)
	}

	var key [8 << 10]byte
	var val [8 << 10]byte
	if _, err := rand.Read(key[:]); err != nil {
		panic(err)
	}
	if _, err := rand.Read(val[:]); err != nil {
		panic(err)
	}

	w := bufio.NewWriter(f)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Encode(w, key[:], val[:]); err != nil {
			panic(err)
		}

	}
	w.Flush()

}

func BenchmarkEncodeFile16KB(b *testing.B) {
	nameFile := "file16.sst"
	f, err := os.OpenFile(nameFile, os.O_CREATE|os.O_RDWR|os.O_APPEND, os.FileMode(0600))
	if err != nil {
		panic(err)
	}

	var key [8 << 10]byte
	var val [8 << 10]byte
	if _, err := rand.Read(key[:]); err != nil {
		panic(err)
	}
	if _, err := rand.Read(val[:]); err != nil {
		panic(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Encode(f, key[:], val[:]); err != nil {
			panic(err)
		}
	}
}

func BenchmarkEncodeBufio32KB(b *testing.B) {
	nameFile := "bufio32.sst"
	f, err := os.OpenFile(nameFile, os.O_CREATE|os.O_RDWR|os.O_APPEND, os.FileMode(0600))
	if err != nil {
		panic(err)
	}

	var key [16 << 10]byte
	var val [16 << 10]byte
	if _, err := rand.Read(key[:]); err != nil {
		panic(err)
	}
	if _, err := rand.Read(val[:]); err != nil {
		panic(err)
	}

	w := bufio.NewWriter(f)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Encode(w, key[:], val[:]); err != nil {
			panic(err)
		}

	}
	w.Flush()

}

func BenchmarkEncodeFile32KB(b *testing.B) {
	nameFile := "file32.sst"
	f, err := os.OpenFile(nameFile, os.O_CREATE|os.O_RDWR|os.O_APPEND, os.FileMode(0600))
	if err != nil {
		panic(err)
	}

	var key [16 << 10]byte
	var val [16 << 10]byte
	if _, err := rand.Read(key[:]); err != nil {
		panic(err)
	}
	if _, err := rand.Read(val[:]); err != nil {
		panic(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Encode(f, key[:], val[:]); err != nil {
			panic(err)
		}
	}
}

func Test_EncodeDecode(t *testing.T) {
	nameFile := "test.sst"
	f, err := os.OpenFile(nameFile, os.O_CREATE|os.O_RDWR, os.FileMode(0600))
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	var key [4]byte
	var val [4]byte

	if n, err := rand.Read(val[:]); err != nil {
		t.Fatal(err)
	} else {
		if n == 0 {
			t.Fatal("n == 0")
		}
	}
	if n, err := rand.Read(key[:]); err != nil {
		t.Fatal(err)
	} else {
		if n == 0 {
			t.Fatal("n == 0")
		}
	}

	//buf := make([]byte, 0, 1<<10)

	tests := []struct {
		name string
		key  []byte
		val  []byte
		i    *bufio.Writer
		o    *bufio.Reader
	}{
		{
			name: "ok",
			key:  key[:],
			val:  val[:],
			i:    bufio.NewWriter(f),
			o:    bufio.NewReader(f),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if n, err := Encode(tt.i, tt.key, tt.val); err != nil {
				t.Fatal(err)
			} else {
				if n == 0 {
					t.Fatal("n == 0")
				}
				log.Println("n write ", n)
			}
			tt.i.Flush()

			if err := f.Close(); err != nil {
				t.Fatal(err)
			}

			f, err = os.OpenFile(nameFile, os.O_CREATE|os.O_RDWR, os.FileMode(0600))
			if err != nil {
				t.Fatal(err)
			}
			tt.o = bufio.NewReader(f)

			if k, v, err := Decode(tt.o); err != nil && err != io.EOF {
				t.Fatal(err)
			} else {
				if !slices.Equal(tt.key, k) {
					t.Fatalf("tt.key --------%d------- != k ------%d-------", len(tt.key), len(k))
				}
				if !slices.Equal(tt.val, v) {
					t.Fatalf("tt.val (%d) != v (%d)", len(tt.val), len(v))
				}
			}
		})
	}
}
