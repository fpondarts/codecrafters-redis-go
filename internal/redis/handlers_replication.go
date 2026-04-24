package redis

import "fmt"

func (r *Redis) handleReplconf() ([]byte, error) {
	return EncodeSimpleString("OK"), nil
}

func (r *Redis) handlePsync() ([]byte, error) {
	return EncodeSimpleString(fmt.Sprintf("FULLRESYNC %s 0", r.replicationID)), nil
}
