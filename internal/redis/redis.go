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

type transaction struct {
	connID      uint64
	commands    []Command
	watchedKeys []string
	multiCalled bool
	dirty       bool // true if a watched key was modified before EXEC
}
type waiter struct {
	ch      chan []byte
	claimed atomic.Bool // CAS to claim; whoever wins is the sole writer to ch
}

type RedisConfig struct {
	IsReplica bool
}
type Redis struct {
	storage      *Storage
	transactions map[uint64]transaction // connID -> queued commands (key present = in MULTI)
	waitersBLPOP map[string][]*waiter   // key -> FIFO list of blocked clients
	waitersXREAD map[string][]*waiter
	config       RedisConfig
}

func NewRedis(config RedisConfig) *Redis {
	return &Redis{
		storage:      NewStorage(),
		transactions: make(map[uint64]transaction),
		waitersBLPOP: make(map[string][]*waiter),
		waitersXREAD: make(map[string][]*waiter),
		config:       config,
	}
}

func (r *Redis) OnDisconnect(connID uint64) {
	delete(r.transactions, connID)
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

	if tx, inTx := r.transactions[connID]; inTx {
		switch cmd.Name {
		case "DISCARD":
			return wrap(r.handleDiscard(connID))
		case "EXEC":
			return r.handleExec(connID)
		case "MULTI":
			return wrap(r.handleMulti(connID))
		case "UNWATCH":
			return wrap(r.handleUnwatch(connID))
		case "WATCH":
			return wrap(r.handleWatch(connID, cmd.Args))
		default:
			tx.commands = append(tx.commands, cmd)
			r.transactions[connID] = tx
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
	case "BLPOP":
		return r.handleBLPop(cmd.Args)
	case "DISCARD":
		return wrap(EncodeError("ERR DISCARD without MULTI"), nil)
	case "ECHO":
		return wrap(r.handleEcho(cmd.Args))
	case "EXEC":
		return wrap(EncodeError("ERR EXEC without MULTI"), nil)
	case "GET":
		return wrap(r.handleGet(cmd.Args))
	case "INCR":
		return wrap(r.handleIncr(cmd.Args))
	case "INFO":
		return wrap(r.handleInfo())
	case "LLEN":
		return wrap(r.handleLLen(cmd.Args))
	case "LPOP":
		return wrap(r.handleLPop(cmd.Args))
	case "LPUSH":
		return wrap(r.handleLPush(cmd.Args))
	case "LRANGE":
		return wrap(r.handleLRange(cmd.Args))
	case "MULTI":
		return wrap(r.handleMulti(connID))
	case "PING":
		return wrap(r.handlePing())
	case "RPUSH":
		return wrap(r.handleRPush(cmd.Args))
	case "SET":
		return wrap(r.handleSet(cmd.Args))
	case "TYPE":
		return r.handleType(cmd.Args)
	case "UNWATCH":
		return wrap(r.handleUnwatch(connID))
	case "WATCH":
		return wrap(r.handleWatch(connID, cmd.Args))
	case "XADD":
		return wrap(r.handleXAdd(cmd.Args))
	case "XRANGE":
		return wrap(r.handleXRange(cmd.Args))
	case "XREAD":
		return r.handleXRead(cmd.Args)
	default:
		log.Printf("unknown command %q", cmd.Name)
		return wrap(EncodeError("ERR unknown command '"+cmd.Name+"'"), nil)
	}
}
