package client

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"time"

	"code.cloudfoundry.org/lager"
	"github.com/pivotal-cf-experimental/streaming-mysql-backup-client/clock"
	"github.com/pivotal-cf-experimental/streaming-mysql-backup-client/config"
	"github.com/pivotal-cf-experimental/streaming-mysql-backup-client/download"
	"github.com/pivotal-cf-experimental/streaming-mysql-backup-client/prepare"
	"github.com/pivotal-cf-experimental/streaming-mysql-backup-client/tarpit"

	"github.com/pivotal-cf-experimental/streaming-mysql-backup-client/cryptkeeper"
	"github.com/pivotal-cf-experimental/streaming-mysql-backup-client/fileutils"
)

type MultiError []error

func (e MultiError) Error() string {
	var buf bytes.Buffer

	if len(e) > 1 {
		buf.WriteString("multiple errors:")
	}
	for _, err := range e {
		buf.WriteString("\n")
		buf.WriteString(err.Error())
	}

	return buf.String()
}

//go:generate counterfeiter . Downloader
type Downloader interface {
	DownloadBackup(url string, streamer download.StreamedWriter) error
}

//go:generate counterfeiter . BackupPreparer
type BackupPreparer interface {
	Command(string) *exec.Cmd
}

type Client struct {
	config               config.Config
	version              int64
	index                int
	metadataArtifactName string
	tarClient            *tarpit.TarClient
	backupPreparer       BackupPreparer
	downloader           Downloader
	logger               lager.Logger
	downloadDirectory    string
	prepareDirectory     string
	encryptDirectory     string
	encryptor            *cryptkeeper.CryptKeeper
	metadataFields       map[string]string
}

func DefaultClient(config config.Config) *Client {
	return NewClient(
		config,
		tarpit.NewSystemTarClient(),
		prepare.DefaultBackupPreparer(),
		download.DefaultDownloadBackup(clock.DefaultClock(), config),
	)
}

func NewClient(config config.Config, tarClient *tarpit.TarClient, backupPreparer BackupPreparer, downloader Downloader) *Client {
	client := &Client{
		config:         config,
		tarClient:      tarClient,
		backupPreparer: backupPreparer,
		downloader:     downloader,
	}
	client.logger = config.Logger
	client.encryptor = cryptkeeper.NewCryptKeeper(config.SymmetricKey)
	client.metadataFields = config.MetadataFields
	return client
}

func (this Client) artifactName() string {
	return fmt.Sprintf("mysql-backup-%d-%d", this.version, this.index)
}

func (this Client) downloadedBackupLocation() string {
	return path.Join(this.downloadDirectory, "unprepared-backup.tar")
}

func (this Client) preparedBackupLocation() string {
	return path.Join(this.encryptDirectory, "prepared-backup.tar")
}

func (this Client) encryptedBackupLocation() string {
	return path.Join(this.config.OutputDir, fmt.Sprintf("%s.tar.gpg", this.artifactName()))
}

func (this Client) originalMetadataLocation() string {
	return path.Join(this.prepareDirectory, "xtrabackup_info")
}

func (this Client) finalMetadataLocation() string {
	return path.Join(this.config.OutputDir, fmt.Sprintf("%s.txt", this.artifactName()))
}

func (this *Client) Execute() error {
	var errors MultiError
	for i := 0; i < len(this.config.Urls); i++ {
		this.version = time.Now().Unix()
		this.index = i
		err := this.runTasksSequentially(
			this.createDirectories,
			this.downloadBackup,
			this.untarBackup,
			this.cleanDownloadDirectory,
			this.prepareBackup,
			this.writeMetadataFile,
			this.archiveBackup,
			this.cleanPrepareDirectory,
			this.encryptBackup,
			this.cleanEncryptDirectory,
		)
		if err != nil {
			errors = append(errors, err)
		}
		this.cleanDirectories() //ensure directories are cleaned on error
	}
	if len(errors) == len(this.config.Urls) {
		return errors
	}
	return nil
}

func (this *Client) runTasksSequentially(tasks ...func() error) error {
	for _, task := range tasks {
		if err := task(); err != nil {
			return err
		}
	}
	this.logger.Info("Successful execution of client tasks")
	return nil
}

func (this *Client) createDirectories() error {
	this.logger.Debug("Creating directories")

	var err error
	this.downloadDirectory, err = ioutil.TempDir(this.config.TmpDir, "mysql-backup-downloads")
	if err != nil {
		this.logger.Error("Error creating temporary directory 'mysql-backup-downloads'", err)
		return err
	}

	this.prepareDirectory, err = ioutil.TempDir(this.config.TmpDir, "mysql-backup-prepare")
	if err != nil {
		this.logger.Error("Error creating temporary directory 'mysql-backup-prepare'", err)
		return err
	}

	this.encryptDirectory, err = ioutil.TempDir(this.config.TmpDir, "mysql-backup-encrypt")
	if err != nil {
		this.logger.Error("Error creating temporary directory 'mysql-backup-encrypt'", err)
		return err
	}

	this.logger.Debug("Created directories", lager.Data{
		"downloadDirectory": this.downloadDirectory,
		"prepareDirectory":  this.prepareDirectory,
		"encryptDirectory":  this.encryptDirectory,
	})

	return nil
}

func (this *Client) downloadBackup() error {
	this.logger.Info("Starting download of backup", lager.Data{
		"backup-temporary-download-path": this.downloadedBackupLocation(),
	})

	streamingFileWriter := fileutils.StreamingFileWriter{
		Filename: this.downloadedBackupLocation(),
	}

	err := this.downloader.DownloadBackup(this.config.Urls[this.index], streamingFileWriter)
	if err != nil {
		this.logger.Error("DownloadBackup failed", err)
		return err
	}
	this.logger.Info("Finished downloading backup", lager.Data{
		"backup-temporary-download-path": this.downloadedBackupLocation(),
	})

	return nil
}

func (this *Client) untarBackup() error {
	unTarCmd := this.tarClient.Untar(this.downloadedBackupLocation(), this.prepareDirectory)
	this.logger.Debug("Untar command", lager.Data{
		"command": unTarCmd,
		"args":    unTarCmd.Args,
	})

	this.logger.Info("Starting untar command", lager.Data{
		"inputFile":       this.downloadedBackupLocation(),
		"outputDirectory": this.prepareDirectory,
	})
	output, err := unTarCmd.CombinedOutput()
	if err != nil {
		this.logger.Error("Command untar failed", err, lager.Data{
			"output": output,
		})
		return err
	}
	this.logger.Info("Finished untar command successfully")

	return nil
}

func (this *Client) prepareBackup() error {
	backupPrepare := this.backupPreparer.Command(this.prepareDirectory)
	this.logger.Debug("Backup prepare command", lager.Data{
		"command": backupPrepare,
		"args":    backupPrepare.Args,
	})

	this.logger.Info("Starting prepare of backup", lager.Data{
		"prepareDirectory": this.prepareDirectory,
	})
	output, err := backupPrepare.CombinedOutput()
	if err != nil {
		this.logger.Error("Preparing the backup failed", err, lager.Data{
			"output": output,
		})
		return err
	}
	this.logger.Info("Successfully prepared a backup")

	return nil
}

// The xtrabackup_info file inside of the backup artifact contains relevant
// metadata information useful to operators, e.g. the effective backup time = `start_time`
//
// Copy this outside of the resultant re-compressed artifact so operators
// can glean this useful information without first downloading the large backup
//
// We had to add a sample xtrabackup_info file to the test fixture because of
// this concrete file dependency
//
// See: https://www.pivotaltracker.com/story/show/98994636
func (this *Client) writeMetadataFile() error {
	src := this.originalMetadataLocation()
	dst := this.finalMetadataLocation()

	this.logger.Info("Copying metadata file", lager.Data{
		"from": src,
		"to":   dst,
	})

	_, err := os.Create(dst)
	if err != nil {
		return err
	}

	backupMetadataMap, err := fileutils.ExtractFileFields(src)
	if err != nil {
		this.logger.Error("Opening xtrabackup-info file failed", err)
		return err
	}

	for key, value := range this.metadataFields {
		backupMetadataMap[key] = value
	}

	for key, value := range backupMetadataMap {
		keyValLine := fmt.Sprintf("%s = %s", key, value)
		err = fileutils.WriteLineToFile(dst, keyValLine)
		if err != nil {
			this.logger.Error("Writing metadata file failed", err)
			return err
		}
	}

	this.logger.Info("Finished writing metadata file")

	return nil
}

func (this *Client) archiveBackup() error {
	this.logger.Info("Starting archiving backup")

	tarCmd := this.tarClient.Tar(this.preparedBackupLocation(), this.prepareDirectory)
	this.logger.Debug("Tar command", lager.Data{
		"command": tarCmd,
		"args":    tarCmd.Args,
	})

	this.logger.Info("Starting tar of backup", lager.Data{
		"prepareDirectory": this.prepareDirectory,
		"outputArtifact":   this.preparedBackupLocation(),
	})
	output, err := tarCmd.CombinedOutput()
	if err != nil {
		this.logger.Error("Command tar failed", err, lager.Data{
			"output": output,
		})
		return err
	}
	this.logger.Info("Successfully compressed backup")

	return nil
}

func (this *Client) encryptBackup() error {
	this.logger.Info("Starting encrypting backup")

	unencryptedFileReader, err := os.Open(this.preparedBackupLocation())
	if err != nil {
		this.logger.Error("Error reading backup file", err)
		return err
	}
	defer unencryptedFileReader.Close()

	encryptedFileWriter, err := os.Create(this.encryptedBackupLocation())
	if err != nil {
		this.logger.Error("Error creating encrypted backup file", err)
		return err
	}
	defer encryptedFileWriter.Close()

	err = this.encryptor.Encrypt(unencryptedFileReader, encryptedFileWriter)
	if err != nil {
		this.logger.Error("Error while encrypting backup file", err)
		return err
	}

	this.logger.Info("Successfully encrypted backup")
	return nil
}

func (this *Client) cleanDownloadDirectory() error {
	err := os.RemoveAll(this.downloadDirectory)
	if err != nil {
		this.logger.Error(fmt.Sprintf("Failed to remove %s", this.downloadDirectory), err)
		return err
	}

	this.logger.Debug("Cleaned download directory")
	return nil
}

func (this *Client) cleanPrepareDirectory() error {
	err := os.RemoveAll(this.prepareDirectory)
	if err != nil {
		this.logger.Error(fmt.Sprintf("Failed to remove %s", this.prepareDirectory), err)
		return err
	}

	this.logger.Debug("Cleaned prepare directory")
	return nil
}

func (this *Client) cleanEncryptDirectory() error {
	err := os.RemoveAll(this.encryptDirectory)
	if err != nil {
		this.logger.Error(fmt.Sprintf("Failed to remove %s", this.encryptDirectory), err)
		return err
	}

	this.logger.Debug("Cleaned encrypt directory")
	return nil
}

func (this *Client) cleanDirectories() error {
	this.logger.Debug("Cleaning directories", lager.Data{
		"downloadDirectory": this.downloadDirectory,
		"prepareDirectory":  this.prepareDirectory,
		"encryptDirectory":  this.encryptDirectory,
	})

	//continue execution even if cleanup fails
	_ = this.cleanDownloadDirectory()
	_ = this.cleanPrepareDirectory()
	_ = this.cleanEncryptDirectory()
	return nil
}
