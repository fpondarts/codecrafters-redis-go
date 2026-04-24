package redis

func appendInfoLine(s, key, val string) string {
	return s + key + ":" + val + "\n"
}

func (r *Redis) handleInfo() ([]byte, error) {
	info := ""

	role := "master"
	if r.config.IsReplica {
		role = "slave"
	}
	info = appendInfoLine(info, "role", role)
	return EncodeBulkString(info), nil
}
