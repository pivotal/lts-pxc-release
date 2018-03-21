package fs

import (
	"os"
)

func CleanDirectory(path string) error {
	err := os.RemoveAll(path)
	if err != nil {
		return err
	}

	return os.Mkdir(path, 0700)
}