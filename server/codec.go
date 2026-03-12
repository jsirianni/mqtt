package server

import (
	"encoding/binary"
	"io"
	"unicode/utf8"
)

const maxRemainingLength = 268435455

// decodeRemainingLength reads 1-4 bytes and returns remaining length and bytes read.
func decodeRemainingLength(r io.Reader) (uint32, int, error) {
	var n uint32
	var mult uint32 = 1
	var read int
	for {
		var b [1]byte
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return 0, read, err
		}
		read++
		n += uint32(b[0]&0x7F) * mult
		if mult > 128*128*128 {
			return 0, read, ErrMalformedPacket
		}
		mult *= 128
		if b[0]&0x80 == 0 {
			break
		}
		if read >= 4 {
			return 0, read, ErrMalformedPacket
		}
	}
	return n, read, nil
}

// encodeRemainingLength encodes remaining length into buf (1-4 bytes). Returns bytes written.
func encodeRemainingLength(buf []byte, n uint32) int {
	if n > maxRemainingLength {
		return 0
	}
	i := 0
	for {
		b := byte(n % 128)
		n /= 128
		if n > 0 {
			b |= 0x80
		}
		buf[i] = b
		i++
		if n == 0 {
			break
		}
	}
	return i
}

// sizeRemainingLength returns how many bytes encodeRemainingLength would write.
func sizeRemainingLength(n uint32) int {
	if n > maxRemainingLength {
		return 0
	}
	c := 0
	for {
		c++
		if n < 128 {
			break
		}
		n /= 128
	}
	return c
}

// readUint16 reads big-endian uint16
func readUint16(r io.Reader) (uint16, error) {
	var b [2]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(b[:]), nil
}

// writeUint16 writes big-endian uint16
func writeUint16(buf []byte, v uint16) {
	binary.BigEndian.PutUint16(buf, v)
}

// readUTF8String reads 2-byte length then UTF-8 bytes. Validates UTF-8.
func readUTF8String(r io.Reader) (string, error) {
	ln, err := readUint16(r)
	if err != nil {
		return "", err
	}
	if ln == 0 {
		return "", nil
	}
	b := make([]byte, ln)
	if _, err := io.ReadFull(r, b); err != nil {
		return "", err
	}
	if !utf8.Valid(b) {
		return "", ErrInvalidUTF8
	}
	return string(b), nil
}

// writeUTF8String writes 2-byte length then UTF-8 bytes.
func writeUTF8String(buf []byte, s string) int {
	writeUint16(buf, uint16(len(s)))
	copy(buf[2:], s)
	return 2 + len(s)
}

// sizeUTF8String returns 2 + len(s)
func sizeUTF8String(s string) int {
	return 2 + len(s)
}

// readVariableByteInteger reads MQTT variable byte integer (same encoding as remaining length).
func readVariableByteInteger(r io.Reader) (uint32, int, error) {
	return decodeRemainingLength(r)
}

// encodeVariableByteInteger encodes into buf. Returns bytes written.
func encodeVariableByteInteger(buf []byte, n uint32) int {
	return encodeRemainingLength(buf, n)
}

func sizeVariableByteInteger(n uint32) int {
	return sizeRemainingLength(n)
}

// readByte reads single byte
func readByte(r io.Reader) (byte, error) {
	var b [1]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return b[0], nil
}
