package redis

import (
	"fmt"
	"os"
	"strings"
)

func (r *Redis) getConfigValue(name string) (string, error) {
	switch name {
	case "appendonly":
		if r.config.AppendOnly == "" {
			return "no", nil
		}
		return r.config.AppendOnly, nil
	case "appenddirname":
		if r.config.AppendDirName == "" {
			return "appendonlydir", nil
		}
		return r.config.AppendDirName, nil
	case "appendfilename":
		if r.config.AppendFileName == "" {
			return "appendonly.aof", nil
		}
		return r.config.AppendFileName, nil
	case "appendfsync":
		if r.config.AppendFsync == "" {
			return "everysec", nil
		}
		return r.config.AppendFsync, nil
	case "dir":
		if r.config.Dir == "" {
			return os.Getwd()
		}
		return r.config.Dir, nil
	case "dbfilename":
		return r.config.DbFileName, nil
	default:
		return "", fmt.Errorf("config not found")
	}
}

func (r *Redis) handleConfig(args []string) ([]byte, error) {
	value, err := r.getConfigValue(strings.ToLower(args[1]))
	if err != nil {
		return EncodeNullArray(), nil
	}
	return EncodeArray([]string{strings.ToLower(args[1]), value}), nil
}
