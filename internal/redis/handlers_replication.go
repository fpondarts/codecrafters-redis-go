package redis

func (r *Redis) handleReplconf() ([]byte, error) {
	return EncodeSimpleString("OK"), nil
}
