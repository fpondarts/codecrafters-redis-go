// Package redis for handling Redis protocol and commands
package redis

import (
	"bufio"
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

// RESPMessage pairs a parsed element with its wire encoding so callers that
// have already parsed a frame don't need to re-encode or re-parse it.
type RESPMessage struct {
	Raw     []byte
	Element RESPElement
}

// NewRESPMessage encodes el once and returns a message holding both representations.
func NewRESPMessage(el RESPElement) RESPMessage {
	return RESPMessage{Raw: EncodeElement(el), Element: el}
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

