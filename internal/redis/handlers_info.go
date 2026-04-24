package redis

func appendInfoLine(s, key, val string) string {
	return s + key + ":" + val + "\n"
}

func (r *Redis) handleInfo() ([]byte, error) {
	info := ""

	role := "master"
	if r.isReplica() {
		role = "slave"
	}
	info = appendInfoLine(info, "role", role)
	info = appendInfoLine(info, "master_replid", r.replicationID)
	info = appendInfoLine(info, "master_repl_offset", "0")
	return EncodeBulkString(info), nil
}
