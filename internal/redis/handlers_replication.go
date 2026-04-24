package redis

func (r *Redis) handleReplconf(args []string) ([]byte, error) {
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
