package redis

import "log"

type Redis struct {
	storage *Storage
}

func NewRedis() *Redis {
	return &Redis{storage: NewStorage()}
}

// Handle is the single entry point for the TCP server. It parses buf as a RESP
// message, dispatches to the correct handler, and returns the RESP-encoded response.
// All Redis-level errors (parse errors, wrong type, unknown command) are encoded
// into the returned bytes — a non-nil Go error signals an unrecoverable failure.
func (r *Redis) Handle(buf []byte) ([]byte, error) {
	el, _, err := ParseRESP(buf)
	if err != nil {
		log.Printf("RESP parse error: %v", err)
		return EncodeError("ERR Protocol error: " + err.Error()), nil
	}
	cmd, err := ParseCommand(el)
	if err != nil {
		log.Printf("command parse error: %v", err)
		return EncodeError("ERR " + err.Error()), nil
	}
	log.Printf("dispatching command %q args=%v", cmd.Name, cmd.Args)
	return r.dispatch(cmd)
}

func (r *Redis) dispatch(cmd Command) ([]byte, error) {
	switch cmd.Name {
	case "PING":
		return r.handlePing()
	case "ECHO":
		return r.handleEcho(cmd.Args)
	case "SET":
		return r.handleSet(cmd.Args)
	case "GET":
		return r.handleGet(cmd.Args)

	case "LPUSH":
		return r.handleLPush(cmd.Args)
	case "RPUSH":
		return r.handleRPush(cmd.Args)
	case "LRANGE":
		return r.handleLRange(cmd.Args)
	case "LLEN":
		return r.handleLLen(cmd.Args)
	default:
		log.Printf("unknown command %q", cmd.Name)
		return EncodeError("ERR unknown command '" + cmd.Name + "'"), nil
	}
}
