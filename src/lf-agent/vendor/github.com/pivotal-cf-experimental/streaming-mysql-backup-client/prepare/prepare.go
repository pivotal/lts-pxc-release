package prepare

import (
	"os/exec"
)

type InnoBackupExPreparer struct {
}

func DefaultBackupPreparer() *InnoBackupExPreparer {
	return &InnoBackupExPreparer{}
}

func (this *InnoBackupExPreparer) Command(backupDir string) *exec.Cmd {
	return exec.Command("innobackupex", "--apply-log", backupDir)
}
