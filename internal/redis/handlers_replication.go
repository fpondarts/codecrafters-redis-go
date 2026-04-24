package redis

import "strings"

func (r *Redis) handleReplconf(args []string) ([]byte, error) {
	if len(args) > 0 && strings.ToUpper(args[0]) == "GETACK" {
		return EncodeArray([]string{"REPLCONF", "ACK", "0"}), nil
	}
	return EncodeSimpleString("OK"), nil
}

func (r *Redis) handlePsync(connID uint64) ([]byte, error) {
	fullresync := EncodeSimpleString("FULLRESYNC " + r.replicationID + " 0")
	rdb := encodeRDB(emptyRDB)
	conn, ok := r.connMap[connID]
	if ok {
		r.replicaConns[connID] = conn
		combined := append(fullresync, rdb...)
		conn.Write(combined)
	}
	return []byte{}, nil
}
