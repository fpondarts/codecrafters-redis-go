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
	err := os.Mkdir(dirPath, 0o755)

	if !os.IsExist(err) {
		return err
	}

	file, err := os.Create(filepath.Join(dirPath, r.config.AppendFileName, ".1.incr.aof"))
	if err != nil {
		log.Fatalf("%v", err)
	}

	log.Printf("FILE: %s", file.Name())
	return err
}
