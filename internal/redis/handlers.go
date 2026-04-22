package redis

import (
	"fmt"
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
	return EncodeInteger(int64(n)), nil
}

func (r *Redis) handleLPush(args []string) ([]byte, error) {
	if len(args) < 2 {
		return EncodeError("ERR wrong number of arguments for 'rpush' command"), nil
	}

	key, vals := args[0], args[1:]
	log.Printf("RPUSH %q %v", key, vals)
	n, err := r.storage.LPush(key, vals...)
	if err != nil {
		return EncodeError(err.Error()), nil
	}

	return EncodeInteger(int64(n)), nil
}

func (r *Redis) handleLRange(args []string) ([]byte, error) {
	if len(args) != 3 {
		return EncodeError(fmt.Sprintf("ERR wrong number of arguments for 'lrange' command")), nil
	}
	start, err := strconv.Atoi(args[1])
	if err != nil {
		return EncodeError("ERR value is not an integer or out of range"), nil
	}
	end, err := strconv.Atoi(args[2])
	if err != nil {
		return EncodeError("ERR value is not an integer or out of range"), nil
	}
	list, err := r.storage.GetListRange(args[0], start, end)
	if err != nil {
		return EncodeError(err.Error()), nil
	}
	log.Printf("LRANGE %q [%d:%d] -> %d elements", args[0], start, end, len(list))
	return EncodeArray(list), nil
}
