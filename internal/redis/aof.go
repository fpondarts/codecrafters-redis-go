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

	fileName := r.config.AppendFileName + ".1.incr.aof"
	filePath := filepath.Join(dirPath, fileName)
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	manifestFilePath := filepath.Join(dirPath, r.config.AppendFileName+".manifest")

	os.WriteFile(manifestFilePath, []byte("file "+fileName+" seq 1 type i"), 0o644)
	log.Printf("AOF file: %s", file.Name())
	return nil
}
