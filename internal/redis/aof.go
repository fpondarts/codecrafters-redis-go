package redis

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
)

func (r *Redis) getAofDirPath() string {
	return filepath.Join(r.config.Dir, r.config.AppendDirName)
}

func (r *Redis) getSeqFilePath(seq string) string {
	return filepath.Join(r.getAofDirPath(), r.getSeqFileName(seq))
}

func (r *Redis) getSeqFileName(seq string) string {
	return r.config.AppendFileName + "." + "seq" + seq + ".incr.aof"
}

func (r *Redis) getManifestFilePath() string {
	return filepath.Join(r.getAofDirPath(), r.config.AppendFileName+".manifest")
}

func (r *Redis) initAof() error {
	if r.config.AppendOnly != "yes" {
		return nil
	}

	dirPath := filepath.Join(r.config.Dir, r.config.AppendDirName)
	if err := os.Mkdir(dirPath, 0o755); err != nil && !os.IsExist(err) {
		return err
	}

	seqFilePath := r.getSeqFilePath("1")
	file, err := os.OpenFile(seqFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := os.WriteFile(r.getManifestFilePath(), []byte("file "+r.getSeqFileName("1")+" seq 1 type i"), 0o644); err != nil {
		return err
	}
	log.Printf("AOF file: %s", file.Name())
	return nil
}

// activeAofPath reads the manifest and returns the path of the active AOF file.
// Manifest format: "file <filename> seq <n> type <t>"
func (r *Redis) activeAofPath() (string, error) {
	data, err := os.ReadFile(r.getManifestFilePath())
	if err != nil {
		return "", err
	}
	fields := strings.Fields(string(data))
	if len(fields) < 2 || fields[0] != "file" {
		return "", fmt.Errorf("invalid AOF manifest format")
	}
	return filepath.Join(r.getAofDirPath(), fields[1]), nil
}

func (r *Redis) writeCommandToAof(cmd []byte) error {
	if r.config.AppendOnly != "yes" {
		return nil
	}

	path, err := r.activeAofPath()
	if err != nil {
		return err
	}

	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(cmd)
	return err
}
