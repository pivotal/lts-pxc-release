package jumpstart

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"lf-agent/config"

	"github.com/pivotal-cf-experimental/streaming-mysql-backup-client/download"
	"github.com/pivotal-cf-experimental/streaming-mysql-backup-client/tarpit"
	"github.com/pkg/errors"
)

type streamingBackupClient struct {
	cfg    config.Config
	logger *log.Logger
}

func NewStreamingBackupClient(cfg config.Config) *streamingBackupClient {
	return &streamingBackupClient{
		cfg:    cfg,
		logger: log.New(os.Stdout, "[streaming backup client] ", log.LstdFlags),
	}
}

func (c *streamingBackupClient) StreamBackup() error {
	tlsConfig, err := c.getTLSConfig()
	if err != nil {
		return err
	}

	leaderAddress := c.cfg.PeerAddress
	downloader := download.NewDownloaderFromCredentials(c.cfg.StreamingBackupHttpUsername, c.cfg.StreamingBackupHttpPassword, tlsConfig)

	untarStreamer := tarpit.NewUntarStreamer(c.cfg.Datadir)
	err = downloader.DownloadBackup(fmt.Sprintf("https://%s:8081/backup", leaderAddress), untarStreamer)
	if err != nil {
		c.logger.Printf("Error received untaring backup from %s into %s: %v", leaderAddress, c.cfg.Datadir, err)
		return errors.Wrapf(err, "Error downloading and untaring backup")
	}

	return nil
}

func (c *streamingBackupClient) getTLSConfig() (*tls.Config, error) {
	caCert, err := ioutil.ReadFile(c.cfg.StreamingBackupCACertPath)
	if err != nil {
		return nil, err
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	tlsConfig := &tls.Config{
		RootCAs:    caCertPool,
		ServerName: c.cfg.StreamingBackupSSLCommonName,
	}

	return tlsConfig, nil
}
