package redis

import (
	"os"
	"path/filepath"
)

func (r *Redis) initAof() error {
	if r.config.AppendOnly != "yes" {
		return nil
	}

	dirPath := filepath.Join(r.config.Dir, r.config.AppendDirName)
	err := os.Mkdir(dirPath, 0o755)

	if !os.IsExist(err) {
		return err
	}

	_, err = os.OpenFile(filepath.Join(dirPath, r.config.AppendFileName, ".1.incr.aof"), os.O_CREATE|os.O_RDWR, 0o755)
	return err
}
