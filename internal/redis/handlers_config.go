package redis

import "strings"

func (r *Redis) handleConfig(args []string) ([]byte, error) {
	if strings.ToLower(args[1]) == "dir" {
		return EncodeArray([]string{"dir", r.config.Dir}), nil
	}

	if strings.ToLower(args[1]) == "dbfilename" {
		return EncodeArray([]string{"dbfilename", r.config.DbFileName}), nil
	}

	return EncodeNullArray(), nil
}
