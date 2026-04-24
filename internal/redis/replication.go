package redis

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
)

// emptyRDB is the minimal valid RDB file (CodeCrafters standard empty snapshot).
var emptyRDB = func() []byte {
	b, _ := hex.DecodeString("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fe00fb0000ff8e01433d49b2078140")
	return b
}()

func encodeRDB(data []byte) []byte {
	header := []byte("$" + strconv.Itoa(len(data)) + "\r\n")
	return append(header, data...)
}

func (r *Redis) connectToMaster() (*net.TCPConn, error) {
	if r.config.Master == nil {
		return nil, nil
	}

	addr := &net.TCPAddr{
		IP:   r.config.Master.IP,
		Port: r.config.Master.Port,
	}

	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return nil, err
	}

	br := bufio.NewReader(conn)
	if err := r.handshake(conn, br); err != nil {
		return nil, err
	}

	r.masterReader = br
	return conn, nil
}

func (r *Redis) handshake(conn *net.TCPConn, br *bufio.Reader) error {
	// Step 1: PING
	if _, err := conn.Write(EncodeArray([]string{"PING"})); err != nil {
		return err
	}
	if err := expectSimpleString(br, "PONG"); err != nil {
		return err
	}

	// Step 2: REPLCONF listening-port <port>
	if _, err := conn.Write(EncodeArray([]string{"REPLCONF", "listening-port", strconv.Itoa(r.config.Port)})); err != nil {
		return err
	}
	if err := expectSimpleString(br, "OK"); err != nil {
		return err
	}

	// Step 3: REPLCONF capa psync2
	if _, err := conn.Write(EncodeArray([]string{"REPLCONF", "capa", "psync2"})); err != nil {
		return err
	}
	if err := expectSimpleString(br, "OK"); err != nil {
		return err
	}

	if _, err := conn.Write(EncodeArray([]string{"PSYNC", "?", "-1"})); err != nil {
		return err
	}

	if err := expectSimpleString(br, ""); err != nil {
		return err
	}

	return nil
}

func expectSimpleString(br *bufio.Reader, expected string) error {
	el, err := ReadRESP(br)
	if err != nil {
		return err
	}
	ss, ok := el.(RESPSimpleString)
	if !ok || (len(expected) != 0 && ss.Value != expected) {
		return fmt.Errorf("expected +%s, got %v", expected, el)
	}
	return nil
}

func (r *Redis) propagateToReplicas(buf []byte) {
	for connID, conn := range r.replicaConns {
		if _, err := conn.Write(buf); err != nil {
			log.Println("Failed to replicate to connID: ", connID)
		}
	}
}

// readRDB drains one RDB transfer from r. The wire format is $<len>\r\n<bytes>
// with no trailing CRLF — distinct from a RESP bulk string.
func readRDB(r *bufio.Reader) error {
	line, err := r.ReadString('\n')
	if err != nil {
		return err
	}
	line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
	if len(line) == 0 || line[0] != '$' {
		return fmt.Errorf("expected RDB header, got %q", line)
	}
	length, err := strconv.Atoi(line[1:])
	if err != nil {
		return fmt.Errorf("invalid RDB length: %w", err)
	}
	_, err = io.ReadFull(r, make([]byte, length))
	return err
}

func (r *Redis) replicaMainLoop() {
	if !r.isReplica() {
		return
	}
	br := r.masterReader
	if err := readRDB(br); err != nil {
		log.Printf("failed to read RDB from master: %v", err)
		return
	}
	for {
		el, err := ReadRESP(br)
		if err != nil {
			log.Printf("lost connection to master: %v", err)
			return
		}
		resp, _ := r.Handle(0, EncodeElement(el))
		if resp.SendToMaster {
			r.masterConn.Write(resp.Data)
		}
	}
}
