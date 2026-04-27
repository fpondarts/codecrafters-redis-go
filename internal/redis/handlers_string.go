package redis

import (
	"log"
	"strconv"
	"strings"
	"time"
)

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
	r.invalidateWatchers(key)
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

func (r *Redis) handleIncr(args []string) ([]byte, error) {
	if len(args) < 1 {
		return EncodeError("Err too few arguments for 'incr' command"), nil
	}

	incr, err := r.storage.Increment(args[0])
	if err != nil {
		return EncodeError("ERR value is not an integer or out of range"), nil
	}
	r.invalidateWatchers(args[0])
	return EncodeInteger(int64(incr)), nil
}

func (r *Redis) handleKeys(args []string) ([]byte, error) {
	if len(args) < 1 {
		return EncodeError("ERR wrong number of arguments for 'keys' command"), nil
	}

	keys, _ := r.storage.Keys()

	return EncodeArray(keys), nil
}
