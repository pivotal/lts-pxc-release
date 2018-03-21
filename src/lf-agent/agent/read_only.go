package agent

import (
	"os"
	"path/filepath"
)

func (lfa *LFAgent) MakeReadOnly() error {
	if err := lfa.databaseClient.MakeReadOnly(); err != nil {
		return err
	}

	err := os.RemoveAll(filepath.Join(lfa.Config.LfStateDir, "leader.cnf"))
	return err
}
