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
	storage      *Storage
	queue        map[uint64][]Command // connID -> queued commands (key present = in MULTI)
	waitersBLPOP map[string][]*waiter // key -> FIFO list of blocked clients
	waitersXREAD map[string][]*waiter
}

func NewRedis() *Redis {
	return &Redis{
		storage:      NewStorage(),
		queue:        make(map[uint64][]Command),
		waitersBLPOP: make(map[string][]*waiter),
		waitersXREAD: make(map[string][]*waiter),
	}
}

func (r *Redis) OnDisconnect(connID uint64) {
	delete(r.queue, connID)
}

// Handle is the single entry point for the TCP server. It parses buf as a RESP
// message, dispatches to the correct handler, and returns a Response.
// All Redis-level errors are encoded into Response.Data — a non-nil Go error
// signals an unrecoverable internal failure.
func (r *Redis) Handle(connID uint64, buf []byte) (Response, error) {
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

	if _, inTx := r.queue[connID]; inTx {
		switch cmd.Name {
		case "EXEC":
			return r.handleExec(connID)
		case "MULTI":
			return wrap(EncodeError("ERR MULTI calls can not be nested"), nil)
		case "DISCARD":
			return wrap(r.handleDiscard(connID))
		default:
			r.queue[connID] = append(r.queue[connID], cmd)
			return wrap(EncodeSimpleString("QUEUED"), nil)
		}
	}

	return r.dispatch(connID, cmd)
}

func wrap(data []byte, err error) (Response, error) {
	return Response{Data: data}, err
}

func (r *Redis) dispatch(connID uint64, cmd Command) (Response, error) {
	switch cmd.Name {
	case "PING":
		return wrap(r.handlePing())
	case "DISCARD":
		return wrap(EncodeError("Err DISCARD without MULTI"), nil)
	case "ECHO":
		return wrap(r.handleEcho(cmd.Args))
	case "SET":
		return wrap(r.handleSet(cmd.Args))
	case "GET":
		return wrap(r.handleGet(cmd.Args))
	case "INCR":
		return wrap(r.handleIncr(cmd.Args))
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
	case "MULTI":
		return wrap(r.handleMulti(connID))
	case "EXEC":
		return wrap(EncodeError("ERR EXEC without MULTI"), nil)
	case "BLPOP":
		return r.handleBLPop(cmd.Args)
	case "XADD":
		return wrap(r.handleXAdd(cmd.Args))
	case "XRANGE":
		return wrap(r.handleXRange(cmd.Args))
	case "XREAD":
		return r.handleXRead(cmd.Args)
	case "TYPE":
		return r.handleType(cmd.Args)
	default:
		log.Printf("unknown command %q", cmd.Name)
		return wrap(EncodeError("ERR unknown command '"+cmd.Name+"'"), nil)
	}
}
