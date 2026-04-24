package redis

func (r *Redis) handleMulti(args []string) ([]byte, error) {
	return EncodeSimpleString("OK"), nil
}
