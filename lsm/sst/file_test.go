package sst

import (
	"os"
	"path"
	"testing"
)

func TestNameBy(t *testing.T) {
	tests := []struct {
		level  Level
		num    uint64
		name   string
		ext    string
		result string
	}{
		{
			level:  10,
			num:    10,
			ext:    ExtBin,
			result: "10-10.bin",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if name := nameBy(tt.level, tt.num, tt.ext); name != tt.result {
				t.Fatalf("name (%s) != result (%s)", name, tt.result)
			}
		})
	}
}

func Test_Levels(t *testing.T) {
	var dirname = "levels-test"
	if err := os.Mkdir(dirname, os.FileMode(0600)); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dirname)

	f1, err := os.OpenFile(path.Join(dirname, "1.0.sst"), os.O_CREATE, os.FileMode(0600))
	if err != nil {
		t.Fatal(err)
	}
	defer f1.Close()

	f2, err := os.OpenFile(path.Join(dirname, "2.0.sst"), os.O_CREATE, os.FileMode(0600))
	if err != nil {
		t.Fatal(err)
	}
	defer f2.Close()

	tests := []struct {
		name    string
		dirname string
		maxLvl  Level
	}{
		{
			name:    "base",
			dirname: dirname,
			maxLvl:  2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if lvls, err := Levels(tt.dirname); err != nil {
				t.Fatal(err)
			} else {
				if len(lvls) != int(tt.maxLvl) {
					t.Fatalf("lvls %d != max lvls %d", len(lvls), tt.maxLvl)
				}
			}
		})
	}
}

func TestParseName(t *testing.T) {
	tests := []struct {
		level Level
		num   uint64
		name  string
		ext   string
		src   string
	}{
		{
			name:  "base",
			level: 10,
			num:   10,
			ext:   ExtBin,
			src:   "10.10.bin",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			level, num, ext, err := ParseName(tt.src)
			if err != nil {
				t.Fatal(err)
			}
			if num != tt.num {
				t.Fatalf("num (%d) != src num (%d)", num, tt.num)
			}
			if level != tt.level {
				t.Fatalf("level (%d) != src level (%d)", level, tt.level)
			}
			if ext != tt.ext {
				t.Fatalf("level (%d) != src level (%d)", level, tt.level)
			}
		})
	}
}
