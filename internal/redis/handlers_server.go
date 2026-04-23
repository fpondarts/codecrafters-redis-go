package redis

import "log"

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
