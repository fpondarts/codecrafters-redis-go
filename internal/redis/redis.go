package redis

import (
	"bufio"
	cryptorand "crypto/rand"
	"encoding/hex"
	"log"
	mathrand "math/rand"
	"net"
	"strings"
	"sync"
	"sync/atomic"
)

var writeCommands = map[string]struct{}{
	"SET":   {},
	"INCR":  {},
	"LPUSH": {},
	"RPUSH": {},
	"LPOP":  {},
	"XADD":  {},
}

func isWriteCommand(name string) bool {
	_, ok := writeCommands[name]
	return ok
}

// Response is returned by Handle. For most commands Data is populated immediately.
// For blocking commands (BLPOP), Pending is set instead and the caller should wait
// on the channel for the response bytes.
type Response struct {
	Data         []byte
	Pending      <-chan []byte
	SendToMaster bool // replica should write Data back to the master connection
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

type MasterNode struct {
	IP   net.IP
	Port int
}
type RedisConfig struct {
	Master         *MasterNode
	Port           int
	Dir            string
	DbFileName     string
	AppendOnly     string // "yes" or "no"
	AppendDirName  string
	AppendFileName string
	AppendFsync    string // "everysec", "always", or "no"
}

type Redis struct {
	storage          *Storage
	transactions     map[uint64]transaction // connID -> queued commands (key present = in MULTI)
	waitersBLPOP     map[string][]*waiter   // key -> FIFO list of blocked clients
	waitersXREAD     map[string][]*waiter
	config           RedisConfig
	replicationID    string // 40-char hex, generated once at startup
	connMap          map[uint64]net.Conn
	connMapMu        sync.RWMutex
	replicas         map[uint64]*replicaState
	replicasMu       sync.RWMutex
	propagatedOffset atomic.Uint64 // total bytes propagated to replicas
	masterConn       *net.TCPConn
	masterReader     *bufio.Reader
	processedBytes   atomic.Uint64 // bytes received from master (replica side)
	aofLoading       bool          // true while replaying AOF — skips re-writing commands
}

func NewRedis(config RedisConfig) *Redis {
	r := &Redis{
		storage:       NewStorage(),
		transactions:  make(map[uint64]transaction),
		waitersBLPOP:  make(map[string][]*waiter),
		waitersXREAD:  make(map[string][]*waiter),
		config:        config,
		replicationID: generateReplID(),
		connMap:       make(map[uint64]net.Conn),
		replicas:      make(map[uint64]*replicaState),
		masterConn:    nil,
	}

	if config.Master != nil {
		conn, err := r.connectToMaster()
		if err != nil {
			return nil
		}
		r.masterConn = conn
	}

	if err := r.loadRdb(); err != nil {
		r.storage = NewStorage()
	}

	r.initAof()
	go r.replicaMainLoop()
	return r
}

func generateReplID() string {
	b := make([]byte, 20)
	if _, err := cryptorand.Read(b); err != nil {
		// fall back to math/rand if crypto/rand is unavailable
		mathrand.Read(b)
	}
	return hex.EncodeToString(b)
}

func (r *Redis) OnConnect(connID uint64, conn net.Conn) {
	r.connMapMu.Lock()
	r.connMap[connID] = conn
	r.connMapMu.Unlock()
}

func (r *Redis) OnDisconnect(connID uint64) {
	delete(r.transactions, connID)

	r.connMapMu.Lock()
	delete(r.connMap, connID)
	r.connMapMu.Unlock()

	r.replicasMu.Lock()
	delete(r.replicas, connID)
	r.replicasMu.Unlock()
}

func (r *Redis) isReplica() bool {
	return r.config.Master != nil
}

// Handle is the single entry point for the TCP server. It parses buf as a RESP
// message, dispatches to the correct handler, and returns a Response.
// All Redis-level errors are encoded into Response.Data — a non-nil Go error
// signals an unrecoverable internal failure.
func (r *Redis) Handle(connID uint64, msg RESPMessage) (Response, error) {
	defer r.processedBytes.Add(uint64(len(msg.Raw)))
	cmd, err := ParseCommand(msg.Element)
	if err != nil {
		log.Printf("command parse error: %v", err)
		return Response{Data: EncodeError("ERR " + err.Error())}, nil
	}
	log.Printf("dispatching command %q args=%v", cmd.Name, cmd.Args)

	if isWriteCommand(cmd.Name) {
		r.propagateToReplicas(msg.Raw)
		if err := r.writeCommandToAof(msg.Raw); err != nil {
			log.Printf("AOF write error: %v", err)
		}
	}
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
	case "CONFIG":
		return wrap(r.handleConfig(cmd.Args))
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
	case "KEYS":
		return wrap(r.handleKeys(cmd.Args))
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
	case "PSYNC":
		return wrap(r.handlePsync(connID))
	case "REPLCONF":
		data, err := r.handleReplconf(connID, cmd.Args)
		isGetAck := len(cmd.Args) > 0 && strings.EqualFold(cmd.Args[0], "GETACK")
		return Response{Data: data, SendToMaster: isGetAck}, err
	case "RPUSH":
		return wrap(r.handleRPush(cmd.Args))
	case "SET":
		return wrap(r.handleSet(cmd.Args))
	case "TYPE":
		return r.handleType(cmd.Args)
	case "UNWATCH":
		return wrap(r.handleUnwatch(connID))
	case "WAIT":
		return r.handleWait(cmd.Args)
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
