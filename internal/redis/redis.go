package redis

import (
	"log"
	"sync/atomic"
)

// Response is returned by Handle. For most commands Data is populated immediately.
// For blocking commands (BLPOP), Pending is set instead and the caller should wait
// on the channel for the response bytes.
type Response struct {
	Data    []byte
	Pending <-chan []byte
}

type waiter struct {
	ch      chan []byte
	claimed atomic.Bool // CAS to claim; whoever wins is the sole writer to ch
}

type Redis struct {
	storage *Storage
	waiters map[string][]*waiter // key -> FIFO list of blocked clients
}

func NewRedis() *Redis {
	return &Redis{storage: NewStorage(), waiters: make(map[string][]*waiter)}
}

// Handle is the single entry point for the TCP server. It parses buf as a RESP
// message, dispatches to the correct handler, and returns a Response.
// All Redis-level errors are encoded into Response.Data — a non-nil Go error
// signals an unrecoverable internal failure.
func (r *Redis) Handle(buf []byte) (Response, error) {
	el, _, err := ParseRESP(buf)
	if err != nil {
		log.Printf("RESP parse error: %v", err)
		return Response{Data: EncodeError("ERR Protocol error: " + err.Error())}, nil
	}
	cmd, err := ParseCommand(el)
	if err != nil {
		log.Printf("command parse error: %v", err)
		return Response{Data: EncodeError("ERR " + err.Error())}, nil
	}
	log.Printf("dispatching command %q args=%v", cmd.Name, cmd.Args)
	return r.dispatch(cmd)
}

func wrap(data []byte, err error) (Response, error) {
	return Response{Data: data}, err
}

func (r *Redis) dispatch(cmd Command) (Response, error) {
	switch cmd.Name {
	case "PING":
		return wrap(r.handlePing())
	case "ECHO":
		return wrap(r.handleEcho(cmd.Args))
	case "SET":
		return wrap(r.handleSet(cmd.Args))
	case "GET":
		return wrap(r.handleGet(cmd.Args))
	case "LPUSH":
		return wrap(r.handleLPush(cmd.Args))
	case "RPUSH":
		return wrap(r.handleRPush(cmd.Args))
	case "LRANGE":
		return wrap(r.handleLRange(cmd.Args))
	case "LLEN":
		return wrap(r.handleLLen(cmd.Args))
	case "LPOP":
		return wrap(r.handleLPop(cmd.Args))
	case "BLPOP":
		return r.handleBLPop(cmd.Args)
	case "XADD":
		return wrap(r.handleXAdd(cmd.Args))
	case "XRANGE":
		return wrap(r.handleXRange(cmd.Args))
	case "XREAD":
		return wrap(r.handleXRead(cmd.Args))
	case "TYPE":
		return r.handleType(cmd.Args)
	default:
		log.Printf("unknown command %q", cmd.Name)
		return wrap(EncodeError("ERR unknown command '"+cmd.Name+"'"), nil)
	}
}
