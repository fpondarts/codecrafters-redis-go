package redis

import (
	"log"
	"strconv"
	"strings"
	"time"
)

func (r *Redis) handlePing() ([]byte, error) {
	log.Printf("PING -> PONG")
	return EncodeSimpleString("PONG"), nil
}

func (r *Redis) handleEcho(args []string) ([]byte, error) {
	if len(args) != 1 {
		return EncodeError("ERR wrong number of arguments for 'echo' command"), nil
	}
	log.Printf("ECHO %q", args[0])
	return EncodeBulkString(args[0]), nil
}

func (r *Redis) handleSet(args []string) ([]byte, error) {
	if len(args) < 2 {
		return EncodeError("ERR wrong number of arguments for 'set' command"), nil
	}
	key, value := args[0], args[1]
	var expiration time.Time
	if len(args) > 2 {
		if len(args) != 4 {
			return EncodeError("ERR syntax error"), nil
		}
		n, err := strconv.ParseInt(args[3], 10, 64)
		if err != nil || n <= 0 {
			return EncodeError("ERR invalid expire time in 'set' command"), nil
		}
		switch strings.ToUpper(args[2]) {
		case "EX":
			expiration = time.Now().Add(time.Duration(n) * time.Second)
		case "PX":
			expiration = time.Now().Add(time.Duration(n) * time.Millisecond)
		default:
			return EncodeError("ERR syntax error"), nil
		}
	}
	log.Printf("SET %q = %q expiration=%v", key, value, expiration)
	if err := r.storage.Set(key, value, expiration); err != nil {
		return EncodeError(err.Error()), nil
	}
	return EncodeSimpleString("OK"), nil
}

func (r *Redis) handleGet(args []string) ([]byte, error) {
	if len(args) != 1 {
		return EncodeError("ERR wrong number of arguments for 'get' command"), nil
	}
	val, ok, err := r.storage.Get(args[0])
	if err != nil {
		return EncodeError(err.Error()), nil
	}
	if !ok {
		log.Printf("GET %q -> nil", args[0])
		return EncodeNullBulkString(), nil
	}
	log.Printf("GET %q -> %q", args[0], val)
	return EncodeBulkString(val), nil
}

func (r *Redis) handleRPush(args []string) ([]byte, error) {
	if len(args) < 2 {
		return EncodeError("ERR wrong number of arguments for 'rpush' command"), nil
	}
	key, vals := args[0], args[1:]
	log.Printf("RPUSH %q %v", key, vals)
	n, err := r.storage.RPush(key, vals...)
	if err != nil {
		return EncodeError(err.Error()), nil
	}
	r.notifyWaiters(key)
	return EncodeInteger(int64(n)), nil
}

func (r *Redis) handleLPush(args []string) ([]byte, error) {
	if len(args) < 2 {
		return EncodeError("ERR wrong number of arguments for 'lpush' command"), nil
	}
	key, vals := args[0], args[1:]
	log.Printf("LPUSH %q %v", key, vals)
	n, err := r.storage.LPush(key, vals...)
	if err != nil {
		return EncodeError(err.Error()), nil
	}
	r.notifyWaiters(key)
	return EncodeInteger(int64(n)), nil
}

// notifyWaiters pops one element for the first unserved waiter on key.
// Called from handleLPush/handleRPush after a successful push.
// Only runs in the EventLoop goroutine so no locking is needed.
func (r *Redis) notifyWaiters(key string) {
	for len(r.waitersBLPOP[key]) > 0 {
		w := r.waitersBLPOP[key][0]
		r.waitersBLPOP[key] = r.waitersBLPOP[key][1:]

		if !w.claimed.CompareAndSwap(false, true) {
			continue // timeout goroutine already claimed this waiter
		}

		popped, _ := r.storage.LPop(key, 1)
		if len(popped) == 0 {
			break
		}
		w.ch <- EncodeArray([]string{key, popped[0]})
		log.Printf("notified BLPOP waiter for %q -> %q", key, popped[0])
		return
	}
	if len(r.waitersBLPOP[key]) == 0 {
		delete(r.waitersBLPOP, key)
	}
}

func (r *Redis) handleLRange(args []string) ([]byte, error) {
	if len(args) != 3 {
		return EncodeError("ERR wrong number of arguments for 'lrange' command"), nil
	}
	start, err := strconv.Atoi(args[1])
	if err != nil {
		return EncodeError("ERR value is not an integer or out of range"), nil
	}
	end, err := strconv.Atoi(args[2])
	if err != nil {
		return EncodeError("ERR value is not an integer or out of range"), nil
	}
	list, err := r.storage.LRange(args[0], start, end)
	if err != nil {
		return EncodeError(err.Error()), nil
	}
	log.Printf("LRANGE %q [%d:%d] -> %d elements", args[0], start, end, len(list))
	return EncodeArray(list), nil
}

func (r *Redis) handleLLen(args []string) ([]byte, error) {
	if len(args) < 1 {
		return EncodeError("ERR wrong number of arguments for 'llen' command"), nil
	}
	length, err := r.storage.LLen(args[0])
	if err != nil {
		return EncodeError(err.Error()), nil
	}
	return EncodeInteger(int64(length)), nil
}

func (r *Redis) handleLPop(args []string) ([]byte, error) {
	if len(args) < 1 {
		return EncodeError("ERR wrong number of arguments for 'lpop' command"), nil
	}
	key := args[0]
	amount := 1
	if len(args) > 1 {
		parsedAmount, err := strconv.Atoi(args[1])
		if err != nil {
			return EncodeError("ERR value is not an integer or out of range"), nil
		}
		amount = parsedAmount
	}
	popped, err := r.storage.LPop(key, amount)
	if err != nil {
		return EncodeError(err.Error()), nil
	}
	if len(popped) == 0 {
		return EncodeNullBulkString(), nil
	}
	if len(popped) == 1 {
		return EncodeBulkString(popped[0]), nil
	}
	return EncodeArray(popped), nil
}

func (r *Redis) handleBLPop(args []string) (Response, error) {
	if len(args) < 1 {
		return Response{Data: EncodeError("ERR wrong number of arguments for 'blpop' command")}, nil
	}
	key := args[0]
	timeoutSecs := 0.0
	if len(args) > 1 {
		parsedTimeout, err := strconv.ParseFloat(args[1], 64)
		if err != nil || parsedTimeout < 0 {
			return Response{Data: EncodeError("ERR timeout is not a float or out of range")}, nil
		}
		timeoutSecs = parsedTimeout
	}

	// Try immediate pop first — no need to block if element is already there
	if popped, _ := r.storage.LPop(key, 1); len(popped) > 0 {
		log.Printf("BLPOP %q -> immediate %q", key, popped[0])
		return Response{Data: EncodeArray([]string{key, popped[0]})}, nil
	}

	// Register waiter in FIFO order
	w := &waiter{ch: make(chan []byte, 1)}
	r.waitersBLPOP[key] = append(r.waitersBLPOP[key], w)
	log.Printf("BLPOP %q -> blocking (timeout=%.3fs)", key, timeoutSecs)

	// Timeout goroutine: races with notifyWaiters via CAS.
	// Only the winner writes to w.ch, guaranteeing exactly one write.
	if timeoutSecs > 0 {
		go func() {
			time.Sleep(time.Duration(timeoutSecs * float64(time.Second)))
			if w.claimed.CompareAndSwap(false, true) {
				w.ch <- EncodeNullArray()
			}
		}()
	}

	return Response{Pending: w.ch}, nil
}

func (r *Redis) handleXAdd(args []string) ([]byte, error) {
	if len(args) < 4 || (len(args)-2)%2 != 0 {
		return EncodeError("ERR wrong number of arguments for 'xadd' command"), nil
	}
	key, id, fields := args[0], args[1], args[2:]
	resultID, err := r.storage.XAdd(key, id, fields)
	if err != nil {
		return EncodeError(err.Error()), nil
	}
	log.Printf("XADD %q %q -> %q", key, id, resultID)
	r.notifyXReadWaiters(key)
	return EncodeBulkString(resultID), nil
}

func (r *Redis) notifyXReadWaiters(key string) {
	for waiterKey, waiters := range r.waitersXREAD {
		parts := strings.Split(waiterKey, ",")
		n := len(parts) / 2
		keys, ids := parts[:n], parts[n:]

		matched := false
		for _, k := range keys {
			if k == key {
				matched = true
				break
			}
		}
		if !matched {
			continue
		}

		for _, waiter := range waiters {
			if !waiter.claimed.CompareAndSwap(false, true) {
				continue
			}
			results := make([][]StreamEntry, n)
			for i, k := range keys {
				results[i], _ = r.storage.XRead(k, ids[i])
			}
			waiter.ch <- EncodeXReadResults(keys, results)
		}
		delete(r.waitersXREAD, waiterKey)
	}
}

func (r *Redis) handleXRange(args []string) ([]byte, error) {
	if len(args) < 3 {
		return EncodeError("ERR too few arguments for 'xrange' command"), nil
	}
	key, start, end := args[0], args[1], args[2]
	entries, err := r.storage.XRange(key, start, end)
	if err != nil {
		return EncodeError(err.Error()), nil
	}
	log.Printf("XRANGE %q [%s..%s] -> %d entries", key, start, end, len(entries))
	return EncodeStreamEntries(entries), nil
}

func (r *Redis) handleXRead(args []string) (Response, error) {
	// Syntax: XREAD BLOCK <milliseconds> STREAMS key1 key2 ... keyN id1 id2 ... idN

	if len(args) < 3 {
		return Response{Data: EncodeError("ERR syntax error")}, nil
	}

	blocking, blockingMS := false, 0

	nextArg := 0
	if strings.ToUpper(args[0]) == "BLOCK" {
		blocking = true
		parsedMS, err := strconv.Atoi(args[1])
		if err != nil {
			return Response{Data: EncodeError("Err syntax error")}, nil
		}
		blockingMS = parsedMS
		nextArg += 2
	}

	if strings.ToUpper(args[nextArg]) != "STREAMS" {
		return Response{Data: EncodeError("Err syntax error")}, nil
	}

	rest := args[nextArg+1:]
	if len(rest)%2 != 0 {
		return Response{Data: EncodeError("ERR syntax error")}, nil
	}
	n := len(rest) / 2
	keys, ids := rest[:n], rest[n:]

	results := make([][]StreamEntry, n)
	hasAny := false
	for i, key := range keys {
		entries, err := r.storage.XRead(key, ids[i])
		if err != nil {
			return Response{Data: EncodeError(err.Error())}, nil
		}
		results[i] = entries
		if len(entries) > 0 {
			hasAny = true
		}
	}

	if hasAny {
		log.Printf("XREAD STREAMS %v %v", keys, ids)
		return Response{Data: EncodeXReadResults(keys, results)}, nil
	}
	if !blocking {
		return Response{Data: EncodeNullArray()}, nil
	}

	xreadWaiterKey := strings.Join(rest, ",")
	waiter := &waiter{ch: make(chan []byte, 1)}
	r.waitersXREAD[xreadWaiterKey] = append(r.waitersXREAD[xreadWaiterKey], waiter)

	if blockingMS > 0 {
		go func() {
			time.Sleep(time.Duration(blockingMS) * time.Millisecond)
			if waiter.claimed.CompareAndSwap(false, true) {
				waiter.ch <- EncodeNullArray()
			}
		}()
	}
	return Response{Pending: waiter.ch}, nil
}

func (r *Redis) handleType(args []string) (Response, error) {
	if len(args) < 1 {
		return Response{Data: EncodeError("Err too few arguments for TYPE command")}, nil
	}

	typeStr := r.storage.Type(args[0])

	if typeStr == "" {
		return Response{Data: EncodeSimpleString("none")}, nil
	}

	return Response{Data: EncodeSimpleString(typeStr)}, nil
}
