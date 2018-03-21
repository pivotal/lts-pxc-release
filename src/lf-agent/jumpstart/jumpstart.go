package jumpstart

import (
	"lf-agent/config"
	"lf-agent/fs"
	"os/exec"
)

//go:generate counterfeiter . DatabaseClient
type DatabaseClient interface {
	ApplyGTIDPurged() error
}

//go:generate counterfeiter . CmdRunner
type CmdRunner interface {
	Run(cmd *exec.Cmd) error
}

//go:generate counterfeiter . MonitClient
type MonitClient interface {
	Start(processName string) error
	Stop(processName string) error
}

//go:generate counterfeiter . StreamingBackupClient
type StreamingBackupClient interface {
	StreamBackup() error
}

const (
	processName    = "mysql"
	xtrabackupPath = "/var/vcap/packages/xtrabackup/bin/xtrabackup"
)

type Jumpstart struct {
	Cfg          config.Config
	db           DatabaseClient
	monit        MonitClient
	runner       CmdRunner
	backupClient StreamingBackupClient
}

func NewJumpstart(db DatabaseClient, monit MonitClient, runner CmdRunner, backupClient StreamingBackupClient, cfg config.Config) *Jumpstart {
	return &Jumpstart{
		Cfg:          cfg,
		db:           db,
		monit:        monit,
		backupClient: backupClient,
		runner:       runner,
	}
}

func (j *Jumpstart) Prepare() error {
	if err := j.monit.Stop(processName); err != nil {
		return err
	}

	return fs.CleanDirectory(j.Cfg.Datadir)
}

func (j *Jumpstart) Perform() error {
	if err := j.backupClient.StreamBackup(); err != nil {
		return err
	}

	cmd := exec.Command(xtrabackupPath, "--prepare", "--target-dir="+j.Cfg.Datadir)
	if err := j.runner.Run(cmd); err != nil {
		return err
	}

	if err := j.monit.Start(processName); err != nil {
		return err
	}

	return j.db.ApplyGTIDPurged()
}
