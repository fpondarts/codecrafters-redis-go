// Package redis for handling Redis protocol and commands
package redis

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
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

func EncodeResponses(responses [][]byte) []byte {
	out := []byte("*" + strconv.Itoa(len(responses)) + "\r\n")
	for _, r := range responses {
		out = append(out, r...)
	}
	return out
}

// EncodeXReadResults encodes a multi-stream XREAD response.
// Only streams with at least one entry are included in the output.
func EncodeXReadResults(keys []string, results [][]StreamEntry) []byte {
	count := 0
	for _, entries := range results {
		if len(entries) > 0 {
			count++
		}
	}
	out := []byte("*" + strconv.Itoa(count) + "\r\n")
	for i, entries := range results {
		if len(entries) == 0 {
			continue
		}
		out = append(out, "*2\r\n"...)
		out = append(out, EncodeBulkString(keys[i])...)
		out = append(out, EncodeStreamEntries(entries)...)
	}
	return out
}

func EncodeStreamEntries(entries []StreamEntry) []byte {
	out := []byte("*" + strconv.Itoa(len(entries)) + "\r\n")
	for _, e := range entries {
		out = append(out, "*2\r\n"...)
		out = append(out, EncodeBulkString(encodeStreamID(e.MS, e.Seq))...)
		out = append(out, EncodeArray(e.Fields)...)
	}
	return out
}

// EncodeElement re-encodes a parsed RESPElement back to its wire representation.
func EncodeElement(el RESPElement) []byte {
	switch v := el.(type) {
	case RESPSimpleString:
		return EncodeSimpleString(v.Value)
	case RESPError:
		return EncodeError(v.Value)
	case RESPInteger:
		return EncodeInteger(v.Value)
	case RESPBulkString:
		if v.Null {
			return EncodeNullBulkString()
		}
		return EncodeBulkString(v.Value)
	case RESPArray:
		if v.Null {
			return EncodeNullArray()
		}
		out := []byte("*" + strconv.Itoa(len(v.Elements)) + "\r\n")
		for _, e := range v.Elements {
			out = append(out, EncodeElement(e)...)
		}
		return out
	default:
		return nil
	}
}

// ReadRESP reads from r until exactly one complete RESP message has arrived.
// It wraps r in a bufio.Reader when needed, so it is safe to call repeatedly
// on the same connection — unread bytes stay buffered for the next call.
// Pass a *bufio.Reader directly to share the buffer across calls.
func ReadRESP(r io.Reader) (RESPElement, error) {
	br, ok := r.(*bufio.Reader)
	if !ok {
		br = bufio.NewReader(r)
	}
	return readElement(br)
}

func readElement(r *bufio.Reader) (RESPElement, error) {
	typeByte, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	switch typeByte {
	case '+':
		line, err := readCRLFLine(r)
		if err != nil {
			return nil, err
		}
		return RESPSimpleString{Value: line}, nil
	case '-':
		line, err := readCRLFLine(r)
		if err != nil {
			return nil, err
		}
		return RESPError{Value: line}, nil
	case ':':
		line, err := readCRLFLine(r)
		if err != nil {
			return nil, err
		}
		n, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid integer: %w", err)
		}
		return RESPInteger{Value: n}, nil
	case '$':
		line, err := readCRLFLine(r)
		if err != nil {
			return nil, err
		}
		length, err := strconv.Atoi(line)
		if err != nil {
			return nil, fmt.Errorf("invalid bulk string length: %w", err)
		}
		if length == -1 {
			return RESPBulkString{Null: true}, nil
		}
		data := make([]byte, length+2) // +2 for trailing CRLF
		if _, err := io.ReadFull(r, data); err != nil {
			return nil, err
		}
		return RESPBulkString{Value: string(data[:length])}, nil
	case '*':
		line, err := readCRLFLine(r)
		if err != nil {
			return nil, err
		}
		count, err := strconv.Atoi(line)
		if err != nil {
			return nil, fmt.Errorf("invalid array length: %w", err)
		}
		if count == -1 {
			return RESPArray{Null: true}, nil
		}
		elements := make([]RESPElement, count)
		for i := range count {
			el, err := readElement(r)
			if err != nil {
				return nil, fmt.Errorf("array element %d: %w", i, err)
			}
			elements[i] = el
		}
		return RESPArray{Elements: elements}, nil
	default:
		return nil, fmt.Errorf("unknown RESP type: %q", typeByte)
	}
}

func readCRLFLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r"), nil
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
