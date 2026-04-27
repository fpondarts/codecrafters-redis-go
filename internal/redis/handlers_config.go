package redis

import (
	"fmt"
	"os"
	"strings"
)

func (r *Redis) getConfigValue(name string) (string, error) {
	switch name {
	case "appenddirname":
		return "appendonlydir", nil
	case "appendfilename":
		return "appendonly.aof", nil
	case "appendfsync":
		return "everysec", nil
	case "appendonly":
		return "no", nil
	case "dir":
		{
			if r.config.Dir == "" {
				return os.Getwd()
			}
			return r.config.Dir, nil
		}
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
