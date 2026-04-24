package redis

func appendInfoLine(s, key, val string) string {
	return s + key + ":" + val + "\n"
}

func (r *Redis) handleInfo() ([]byte, error) {
	info := ""
	info = appendInfoLine(info, "role", "master")
	return EncodeBulkString(info), nil
}
