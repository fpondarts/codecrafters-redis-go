package redis

import (
	"os"
	"path/filepath"
)

func (r *Redis) initAof() error {
	if r.config.AppendOnly != "yes" {
		return nil
	}

	err := os.Mkdir(filepath.Join(r.config.Dir, r.config.AppendDirName), 0o755)
	if os.IsExist(err) {
		return nil
	}
	return err
}
