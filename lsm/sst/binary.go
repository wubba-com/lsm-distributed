package sst

import (
	"encoding/binary"
	"fmt"
	"io"
)

// encode encodes key and value and writes it to the specified writer.
// Returns the number of bytes written and error if occurred.
// The function must be compatible with decode: encode(decode(v)) == v.
func Encode(w io.Writer, key []byte, value []byte) (int, error) {
	// encoding format:
	// [encoded total length in bytes][encoded key length in bytes][key][value]

	// number of bytes written
	bytes := 0

	keyLen := encodeUInt64(uint64(len(key)))
	len := len(keyLen) + len(key) + len(value)
	encodedLen := encodeUInt64(uint64(len))

	if n, err := w.Write(encodedLen); err != nil {
		return n, err
	} else {
		bytes += n
	}

	if n, err := w.Write(keyLen); err != nil {
		return bytes + n, err
	} else {
		bytes += n
	}

	if n, err := w.Write(key); err != nil {
		return bytes + n, err
	} else {
		bytes += n
	}

	if n, err := w.Write(value); err != nil {
		return bytes + n, err
	} else {
		bytes += n
	}

	return bytes, nil
}

// decode decodes key and value by reading from the specified reader.
// Returns the number of bytes read and error if occurred.
// The function must be compatible with encode: encode(decode(v)) == v.
func Decode(r io.Reader) ([]byte, []byte, error) {
	// encoding format:
	// [encoded total length in bytes][encoded key length in bytes][key][value]

	var encdodedTotalLen [8]byte
	if n, err := r.Read(encdodedTotalLen[:]); err != nil {
		return nil, nil, err
	} else {
		if n < len(encdodedTotalLen) {
			return nil, nil, fmt.Errorf("the file is corrupted, failed to read entry %d < %d", n, len(encdodedTotalLen))
		}
	}

	bytes := decodeUInt64(encdodedTotalLen[:])
	buf := make([]byte, bytes)

	// var (
	// 	n   int
	// 	err error
	// )
	// for {
	// 	var buf = make([]byte, 4<<10)
	// 	var nn int
	// 	nn, err = r.Read(buf)
	// 	if err != nil {
	// 		if err != io.EOF {
	// 			return nil, nil, err
	// 		}
	// 		break
	// 	}

	// 	copy(encodedEntry[n:], buf[:nn])
	// 	n += nn
	// }
	n, err := io.ReadFull(r, buf)

	if uint64(n) < bytes {
		return nil, nil, fmt.Errorf("the file is corrupted, failed to read entry %d < %d", n, bytes)
	}

	keyLen := decodeUInt64(buf[0:8])
	key := buf[8 : 8+keyLen]
	keyPartLen := 8 + keyLen

	if int(keyPartLen) == len(buf) {
		return key, nil, err
	}

	valueStart := keyPartLen
	value := buf[valueStart:]

	return key, value, nil
}

// encodeKeyOffset encodes key offset and writes it to the given writer.
func EncodeKeyOffset(w io.Writer, key []byte, offset int) (int, error) {
	return Encode(w, key, encodeUInt64(uint64(offset)))
}

func writeUint64(w io.Writer, x uint64) (int, error) {
	return w.Write(encodeUInt64(x))
}

func readUint64(r io.Reader) (uint64, error) {
	var decode [8]byte
	n, err := r.Read(decode[:])
	if err != nil {
		return 0, err
	}
	if n < len(decode) {
		return 0, fmt.Errorf("read %d less than required %d", n, len(decode))
	}

	return decodeUInt64(decode[:]), nil
}

// encodeInt encodes the int as a slice of bytes.
// Must be compatible with decodeInt.
func encodeUInt64(x uint64) []byte {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], x)

	return encoded[:]
}

// decodeInt decodes the slice of bytes as an int.
// Must be compatible with encodeInt.
func decodeUInt64(encoded []byte) uint64 {
	return binary.BigEndian.Uint64(encoded)
}

func readUint32(r io.Reader) (uint64, error) {
	var decode [4]byte
	n, err := r.Read(decode[:])
	if err != nil {
		return 0, err
	}
	if n < len(decode) {
		return 0, fmt.Errorf("read %d less than required %d", n, len(decode))
	}

	return decodeUInt64(decode[:]), nil
}

func writeUint32(w io.Writer, x uint32) (int, error) {
	return w.Write(encodeUInt32(x))
}

// decodeUInt32 decodes the slice of bytes as an int.
// Must be compatible with encodeInt.
func decodeUInt32(encoded []byte) uint32 {
	return binary.BigEndian.Uint32(encoded)
}

// encodeUInt32 encodes the int as a slice of bytes.
// Must be compatible with decodeInt.
func encodeUInt32(x uint32) []byte {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], x)

	return encoded[:]
}

func WriteUInt32Pair(w io.Writer, x, y uint32) (int, error) {
	return w.Write(EncodeUint32Pair(x, y))
}

// encodeIntPair encodes two ints.
func EncodeUint32Pair(x, y uint32) []byte {
	var encoded [8]byte
	binary.BigEndian.PutUint32(encoded[0:4], x)
	binary.BigEndian.PutUint32(encoded[4:], y)

	return encoded[:]
}

// decodeIntPair decodes two ints.
func DecodeUint32Pair(encoded []byte) (uint32, uint32) {
	x := binary.BigEndian.Uint32(encoded[0:4])
	y := binary.BigEndian.Uint32(encoded[4:])

	return x, y
}
