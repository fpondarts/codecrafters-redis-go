package redis

import (
	"log"
	"os"
	"path/filepath"
)

func (r *Redis) initAof() error {
	if r.config.AppendOnly != "yes" {
		return nil
	}

	dirPath := filepath.Join(r.config.Dir, r.config.AppendDirName)
	if err := os.Mkdir(dirPath, 0o755); err != nil && !os.IsExist(err) {
		return err
	}

	filePath := filepath.Join(dirPath, r.config.AppendFileName+".1.incr.aof")
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	log.Printf("AOF file: %s", file.Name())
	return nil
}
