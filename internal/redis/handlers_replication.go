package redis

import (
	"strconv"
	"strings"
)

func (r *Redis) handleReplconf(args []string) ([]byte, error) {
	if len(args) > 0 && strings.ToUpper(args[0]) == "GETACK" {
		return EncodeArray([]string{"REPLCONF", "ACK", strconv.FormatUint(r.processedBytes, 10)}), nil
	}
	return EncodeSimpleString("OK"), nil
}

func (r *Redis) handlePsync(connID uint64) ([]byte, error) {
	fullresync := EncodeSimpleString("FULLRESYNC " + r.replicationID + " 0")
	rdb := encodeRDB(emptyRDB)

	r.connMapMu.RLock()
	conn, ok := r.connMap[connID]
	r.connMapMu.RUnlock()

	if ok {
		r.replicaConnsMu.Lock()
		r.replicaConns[connID] = conn
		r.replicaConnsMu.Unlock()

		combined := append(fullresync, rdb...)
		conn.Write(combined)
	}
	return []byte{}, nil
}

func (r *Redis) handleWait() ([]byte, error) {
	return EncodeInteger(int64(len(r.replicaConns))), nil
}
