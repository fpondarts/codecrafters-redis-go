package redis

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

type dbValue struct {
	vType          byte
	key            string
	value          any
	expirationTime time.Time
}

func readDbSubsection(buf *bufio.Reader, s *Storage) error {
	selector, _ := buf.Peek(1)

	if selector[0] != 0xFE {
		return nil
	}
	buf.Discard(2) // READ DB Number

	for {
		b, err := buf.Peek(1)
		if err != nil {
			return err
		}

		switch b[0] {
		case 0xFE:
			{
				buf.Discard(1)
				return nil
			}
		case 0xFB:
			readDBHashSection(buf)
		default:
			{
				v := readKeyValuePair(buf)
				value := v.value.(string)
				s.Set(v.key, value, v.expirationTime)
			}
		}
	}
}

func readDBHashSection(buf *bufio.Reader) {
	b, _ := buf.Peek(1)

	if b[0] != 0xFB {
		return
	}

	_, _ = buf.Discard(3)
}

func readKeyValuePair(buf *bufio.Reader) dbValue {
	var expirationTime time.Time = time.Time{}

	// Expiration in miliseconds
	if b, _ := buf.Peek(1); b[0] == 0xFC {
		_, _ = buf.Discard(1)
		timeBuf := make([]byte, 8)
		io.ReadFull(buf, timeBuf)
		ms := binary.LittleEndian.Uint64(timeBuf)
		expirationTime = time.Unix(0, int64(ms))
	}

	// Expiration in seconds
	if b, _ := buf.Peek(1); b[0] == 0xFD {
		_, _ = buf.Discard(1)
		timeBuf := make([]byte, 4)
		io.ReadFull(buf, timeBuf)
		sec := binary.LittleEndian.Uint32(timeBuf)
		expirationTime = time.Unix(int64(sec), 0)
	}

	vType, _ := buf.ReadByte()
	key, _ := readEncodedString(buf)
	val := dbValue{expirationTime: expirationTime, vType: vType, key: key}

	switch vType {
	case 0x00:
		{
			v, _ := readEncodedString(buf)
			val.value = v
		}
	}

	return val
}

func readEncodedLength(buf *bufio.Reader) (uint64, error) {
	b, _ := buf.Peek(1)
	lenType := b[0] >> 6

	if lenType == 0x00 {
		l := uint8(b[0] & 0x3f)
		return uint64(l), nil
	}

	if lenType == 0x01 {
		b = make([]byte, 2)
		io.ReadFull(buf, b)
		l := binary.BigEndian.Uint16([]byte{b[0] & 0x3f, b[1]})
		return uint64(l), nil
	}

	if lenType == 0x02 {
		b = make([]byte, 5)
		io.ReadFull(buf, b)
		l := binary.BigEndian.Uint32(b[1:])
		return uint64(l), nil
	}

	return 0, fmt.Errorf("not encoded length")
}

func readEncodedString(buf *bufio.Reader) (string, error) {
	b, _ := buf.Peek(1)
	lenType := b[0] >> 6

	if lenType < 0x03 {
		l, _ := readEncodedLength(buf)
		sBuf := make([]byte, l)
		io.ReadFull(buf, sBuf)
		return string(sBuf), nil
	}

	buf.Discard(1)
	switch b[0] {
	case 0xC0:
		{
			var i int8
			if err := binary.Read(buf, binary.LittleEndian, &i); err != nil {
				return "", err
			}
			return strconv.Itoa(int(i)), nil
		}
	case 0xC1:
		{
			var i int16
			if err := binary.Read(buf, binary.LittleEndian, &i); err != nil {
				return "", err
			}
			return strconv.Itoa(int(i)), nil
		}
	case 0xC2:
		{
			var i int32
			if err := binary.Read(buf, binary.LittleEndian, &i); err != nil {
				return "", err
			}
			return strconv.Itoa(int(i)), nil
		}
	}

	return "", fmt.Errorf("not encoded string")
}

func (r *Redis) loadRdb() error {
	if r.config.DbFileName == "" || r.config.Dir == "" {
		return nil
	}

	filePath := filepath.Join(r.config.Dir, r.config.DbFileName)
	file, err := os.Open(filePath)
	buf := bufio.NewReader(file)

	if err != nil {
		return fmt.Errorf("non existing rdb file")
	}

	for {
		_, err := buf.ReadString(0xFE)
		if err != nil {
			break
		}
		if err := buf.UnreadByte(); err != nil {
			break
		}
		readDbSubsection((buf), r.storage)
	}

	return nil
}
