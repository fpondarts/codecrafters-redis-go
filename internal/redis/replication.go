package redis

import (
	"net"
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
	_, err := conn.Write(EncodeArray([]string{"PING"}))

	return err
}
