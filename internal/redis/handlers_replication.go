package redis

import (
	"log"
	"strconv"
	"strings"
	"time"
)

func (r *Redis) handleReplconf(connID uint64, args []string) ([]byte, error) {
	if len(args) < 1 {
		return EncodeSimpleString("OK"), nil
	}
	switch strings.ToUpper(args[0]) {
	case "GETACK":
		return EncodeArray([]string{"REPLCONF", "ACK", strconv.FormatUint(r.processedBytes.Load(), 10)}), nil
	case "ACK":
		if len(args) >= 2 {
			if offset, err := strconv.ParseUint(args[1], 10, 64); err == nil {
				r.replicasMu.RLock()
				state, ok := r.replicas[connID]
				r.replicasMu.RUnlock()
				if ok {
					state.ackedOffset.Store(offset)
					log.Printf("replica connID=%d acked offset=%d", connID, offset)
				}
			}
		}
		return nil, nil // no response sent back to replica
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
		r.replicasMu.Lock()
		r.replicas[connID] = &replicaState{conn: conn}
		r.replicasMu.Unlock()

		combined := append(fullresync, rdb...)
		conn.Write(combined)
	}
	return []byte{}, nil
}

func (r *Redis) handleWait(args []string) (Response, error) {
	if len(args) < 2 {
		return wrap(EncodeError("ERR wrong number of arguments for 'wait' command"), nil)
	}
	numReplicas, err := strconv.Atoi(args[0])
	if err != nil {
		return wrap(EncodeError("ERR value is not an integer"), nil)
	}
	timeoutMS, err := strconv.Atoi(args[1])
	if err != nil {
		return wrap(EncodeError("ERR value is not an integer"), nil)
	}

	offset := r.propagatedOffset.Load()

	log.Println("Waiting for: ", numReplicas, " replicas to have acked: ", offset, " bytes", numReplicas, offset)
	// No writes propagated yet — all replicas are trivially in sync.
	if offset == 0 {
		r.replicasMu.RLock()
		count := len(r.replicas)
		r.replicasMu.RUnlock()
		return wrap(EncodeInteger(int64(count)), nil)
	}

	// Already enough replicas caught up — return immediately.
	if r.countAckedReplicas(offset) >= numReplicas {
		return wrap(EncodeInteger(int64(numReplicas)), nil)
	}

	// Ask all replicas for their current offset.
	getack := EncodeArray([]string{"REPLCONF", "GETACK", "*"})
	r.replicasMu.RLock()
	for _, state := range r.replicas {
		state.conn.Write(getack)
	}
	r.replicasMu.RUnlock()

	ch := make(chan []byte, 1)
	go func() {
		var deadline <-chan time.Time
		if timeoutMS > 0 {
			deadline = time.After(time.Duration(timeoutMS) * time.Millisecond)
		}
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-deadline:
				ch <- EncodeInteger(int64(r.countAckedReplicas(offset)))
				return
			case <-ticker.C:
				if count := r.countAckedReplicas(offset); count >= numReplicas {
					ch <- EncodeInteger(int64(count))
					return
				}
			}
		}
	}()

	return Response{Pending: ch}, nil
}

func (r *Redis) countAckedReplicas(offset uint64) int {
	r.replicasMu.RLock()
	defer r.replicasMu.RUnlock()
	count := 0
	for _, state := range r.replicas {
		if state.ackedOffset.Load() >= offset {
			count++
		}
	}
	return count
}
