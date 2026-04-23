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
	for len(r.waiters[key]) > 0 {
		w := r.waiters[key][0]
		r.waiters[key] = r.waiters[key][1:]

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
	if len(r.waiters[key]) == 0 {
		delete(r.waiters, key)
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
	r.waiters[key] = append(r.waiters[key], w)
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
