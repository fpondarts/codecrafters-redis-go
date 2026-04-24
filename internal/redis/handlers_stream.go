package redis

import (
	"log"
	"slices"
	"strconv"
	"strings"
	"time"
)

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
	r.invalidateWatchers(key)
	return EncodeBulkString(resultID), nil
}

func (r *Redis) notifyXReadWaiters(key string) {
	for waiterKey, waiters := range r.waitersXREAD {
		parts := strings.Split(waiterKey, ",")
		n := len(parts) / 2
		keys, ids := parts[:n], parts[n:]

		matched := slices.Contains(keys, key)
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
	// Syntax: XREAD [BLOCK <milliseconds>] STREAMS key1 ... keyN id1 ... idN
	if len(args) < 3 {
		return Response{Data: EncodeError("ERR syntax error")}, nil
	}

	blocking, blockingMS := false, 0

	nextArg := 0
	if strings.ToUpper(args[0]) == "BLOCK" {
		blocking = true
		parsedMS, err := strconv.Atoi(args[1])
		if err != nil {
			return Response{Data: EncodeError("ERR syntax error")}, nil
		}
		blockingMS = parsedMS
		nextArg += 2
	}

	if strings.ToUpper(args[nextArg]) != "STREAMS" {
		return Response{Data: EncodeError("ERR syntax error")}, nil
	}

	rest := args[nextArg+1:]
	if len(rest)%2 != 0 {
		return Response{Data: EncodeError("ERR syntax error")}, nil
	}
	n := len(rest) / 2
	keys, ids := rest[:n], rest[n:]

	results := make([][]StreamEntry, n)
	hasAny := false

	for i, id := range ids {
		if id != "$" {
			continue
		}

		entry, err := r.storage.GetLastEntry(keys[i])
		if err != nil {
			return Response{Data: EncodeError(err.Error())}, nil
		}

		ids[i] = encodeStreamID(entry.MS, entry.Seq)
	}

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
	w := &waiter{ch: make(chan []byte, 1)}
	r.waitersXREAD[xreadWaiterKey] = append(r.waitersXREAD[xreadWaiterKey], w)

	if blockingMS > 0 {
		go func() {
			time.Sleep(time.Duration(blockingMS) * time.Millisecond)
			if w.claimed.CompareAndSwap(false, true) {
				w.ch <- EncodeNullArray()
			}
		}()
	}
	return Response{Pending: w.ch}, nil
}
