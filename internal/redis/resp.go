// Package redis for handling Redis protocol and commands
package redis

import (
	"bytes"
	"fmt"
	"strconv"
)

type RESPElement interface {
	Type() string
}

type RESPSimpleString struct {
	Value string
}

func (r RESPSimpleString) Type() string { return "simple_string" }

type RESPError struct {
	Value string
}

func (r RESPError) Type() string { return "error" }

type RESPInteger struct {
	Value int64
}

func (r RESPInteger) Type() string { return "integer" }

type RESPBulkString struct {
	Value string
	Null  bool
}

func (r RESPBulkString) Type() string { return "bulk_string" }

type RESPArray struct {
	Elements []RESPElement
	Null     bool
}

func (r RESPArray) Type() string { return "array" }

func EncodeArray(elements []string) []byte {
	out := []byte("*" + strconv.Itoa(len(elements)) + "\r\n")
	for _, el := range elements {
		out = append(out, EncodeBulkString(el)...)
	}
	return out
}

func EncodeInteger(n int64) []byte {
	return []byte(":" + strconv.FormatInt(n, 10) + "\r\n")
}

func EncodeError(msg string) []byte {
	return []byte("-" + msg + "\r\n")
}

func EncodeSimpleString(s string) []byte {
	return []byte("+" + s + "\r\n")
}

func EncodeBulkString(s string) []byte {
	return []byte("$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n")
}

func EncodeNullBulkString() []byte {
	return []byte("$-1\r\n")
}

func EncodeNullArray() []byte {
	return []byte("*-1\r\n")
}

func EncodeStreamEntries(entries []StreamEntry) []byte {
	out := []byte("*" + strconv.Itoa(len(entries)) + "\r\n")
	for _, e := range entries {
		out = append(out, "*2\r\n"...)
		out = append(out, EncodeBulkString(encodeStreamId(e.MS, e.Seq))...)
		out = append(out, EncodeArray(e.Fields)...)
	}
	return out
}

// ParseRESP parses a RESP-encoded buffer and returns the parsed element and
// the number of bytes consumed.
func ParseRESP(buf []byte) (RESPElement, int, error) {
	if len(buf) == 0 {
		return nil, 0, fmt.Errorf("empty buffer")
	}

	switch buf[0] {
	case '+':
		return parseInline(buf, func(s string) RESPElement { return RESPSimpleString{Value: s} })
	case '-':
		return parseInline(buf, func(s string) RESPElement { return RESPError{Value: s} })
	case ':':
		return parseInline(buf, func(s string) RESPElement {
			n, _ := strconv.ParseInt(s, 10, 64)
			return RESPInteger{Value: n}
		})
	case '$':
		return parseBulkString(buf)
	case '*':
		return parseArray(buf)
	default:
		return nil, 0, fmt.Errorf("unknown RESP type: %q", buf[0])
	}
}

func parseInline(buf []byte, build func(string) RESPElement) (RESPElement, int, error) {
	end := bytes.Index(buf, []byte("\r\n"))
	if end == -1 {
		return nil, 0, fmt.Errorf("missing CRLF")
	}
	return build(string(buf[1:end])), end + 2, nil
}

func parseBulkString(buf []byte) (RESPElement, int, error) {
	end := bytes.Index(buf, []byte("\r\n"))
	if end == -1 {
		return nil, 0, fmt.Errorf("missing CRLF")
	}
	length, err := strconv.Atoi(string(buf[1:end]))
	if err != nil {
		return nil, 0, fmt.Errorf("invalid bulk string length: %w", err)
	}
	if length == -1 {
		return RESPBulkString{Null: true}, end + 2, nil
	}
	start := end + 2
	if len(buf) < start+length+2 {
		return nil, 0, fmt.Errorf("incomplete bulk string")
	}
	return RESPBulkString{Value: string(buf[start : start+length])}, start + length + 2, nil
}

func parseArray(buf []byte) (RESPElement, int, error) {
	end := bytes.Index(buf, []byte("\r\n"))
	if end == -1 {
		return nil, 0, fmt.Errorf("missing CRLF")
	}
	count, err := strconv.Atoi(string(buf[1:end]))
	if err != nil {
		return nil, 0, fmt.Errorf("invalid array length: %w", err)
	}
	if count == -1 {
		return RESPArray{Null: true}, end + 2, nil
	}
	offset := end + 2
	elements := make([]RESPElement, 0, count)
	for i := range count {
		elem, n, err := ParseRESP(buf[offset:])
		if err != nil {
			return nil, 0, fmt.Errorf("parsing array element %d: %w", i, err)
		}
		elements = append(elements, elem)
		offset += n
	}
	return RESPArray{Elements: elements}, offset, nil
}
