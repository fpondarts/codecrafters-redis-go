package redis

import (
	"bufio"
	"fmt"
	"io"
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
	return r.config.AppendFileName + "." + seq + ".incr.aof"
}

func (r *Redis) getManifestFilePath() string {
	return filepath.Join(r.getAofDirPath(), r.config.AppendFileName+".manifest")
}

func (r *Redis) initAof() error {
	if r.config.AppendOnly != "yes" {
		return nil
	}

	dirPath := r.getAofDirPath()
	if err := os.Mkdir(dirPath, 0o755); err != nil && !os.IsExist(err) {
		return err
	}

	if _, err := os.Stat(r.getManifestFilePath()); err == nil {
		// Manifest exists — replay commands from existing AOF.
		if err := r.loadAof(); err != nil {
			log.Printf("AOF load error: %v", err)
		}
	} else {
		// Fresh start — create the incremental file and manifest.
		file, err := os.OpenFile(r.getSeqFilePath("1"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return err
		}
		file.Close()

		if err := os.WriteFile(r.getManifestFilePath(), []byte("file "+r.getSeqFileName("1")+" seq 1 type i"), 0o644); err != nil {
			return err
		}
		log.Printf("AOF initialised: %s", r.getSeqFilePath("1"))
	}
	return nil
}

// activeAofPath reads the manifest and returns the path of the incremental (type i) AOF file.
// Manifest line format: "file <filename> seq <n> type <t>"
func (r *Redis) activeAofPath() (string, error) {
	data, err := os.ReadFile(r.getManifestFilePath())
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		// fields: file <name> seq <n> type <t>
		if len(fields) >= 6 && fields[0] == "file" && fields[5] == "i" {
			return filepath.Join(r.getAofDirPath(), fields[1]), nil
		}
	}
	return "", fmt.Errorf("no incremental AOF entry found in manifest")
}

func (r *Redis) loadAof() error {
	path, err := r.activeAofPath()
	if err != nil {
		return err
	}

	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	r.aofLoading = true
	defer func() { r.aofLoading = false }()

	br := bufio.NewReader(file)
	count := 0
	for {
		el, err := ReadRESP(br)
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("AOF parse error at command %d: %w", count, err)
		}
		r.Handle(0, EncodeElement(el))
		count++
	}

	log.Printf("AOF: replayed %d commands from %s", count, path)
	return nil
}

func (r *Redis) writeCommandToAof(cmd []byte) error {
	if r.config.AppendOnly != "yes" || r.aofLoading {
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
