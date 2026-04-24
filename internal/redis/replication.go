package redis

import (
	"fmt"
	"net"
	"strconv"
)

func (r *Redis) connectToMaster() error {
	if r.config.Master == nil {
		return nil
	}

	addr := &net.TCPAddr{
		IP:   r.config.Master.IP,
		Port: r.config.Master.Port,
	}

	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return err
	}

	return r.handshake(conn)
}

func (r *Redis) handshake(conn *net.TCPConn) error {
	// Step 1: PING
	if _, err := conn.Write(EncodeArray([]string{"PING"})); err != nil {
		return err
	}
	if err := expectSimpleString(conn, "PONG"); err != nil {
		return err
	}

	// Step 2: REPLCONF listening-port <port>
	if _, err := conn.Write(EncodeArray([]string{"REPLCONF", "listening-port", strconv.Itoa(r.config.Port)})); err != nil {
		return err
	}
	if err := expectSimpleString(conn, "OK"); err != nil {
		return err
	}

	// Step 3: REPLCONF capa psync2
	if _, err := conn.Write(EncodeArray([]string{"REPLCONF", "capa", "psync2"})); err != nil {
		return err
	}
	if err := expectSimpleString(conn, "OK"); err != nil {
		return err
	}

	if _, err := conn.Write(EncodeArray([]string{"PSYNC", "?", "-1"})); err != nil {
		return err
	}

	if err := expectSimpleString(conn, ""); err != nil {
		return err
	}

	return nil
}

func expectSimpleString(conn *net.TCPConn, expected string) error {
	buf := make([]byte, 64)
	n, err := conn.Read(buf)
	if err != nil {
		return err
	}
	el, _, err := ParseRESP(buf[:n])
	if err != nil {
		return err
	}
	ss, ok := el.(RESPSimpleString)
	if !ok || (len(expected) != 0 && ss.Value != expected) {
		return fmt.Errorf("expected +%s, got %v", expected, el)
	}
	return nil
}
