package config

import (
	"io/ioutil"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

type Config struct {
	HostAddress                  string `yaml:"host_address"`
	PeerAddress                  string `yaml:"peer_address"`
	Datadir                      string `yaml:"data_dir"`
	EnableHeartbeats             bool   `yaml:"enable_heartbeats"`
	LfStateDir                   string `yaml:"lf_state_dir"`
	ReplicationAdminUser         string `yaml:"replication_admin_user"`
	ReplicationAdminPassword     string `yaml:"replication_admin_password"`
	ReplicationUser              string `yaml:"replication_user"`
	ReplicationPassword          string `yaml:"replication_password"`
	ReplicationWaitSeconds       int    `yaml:"replication_wait_timeout_in_seconds"`
	ReplicationMode              string `yaml:"replication_mode"`
	Port                         string `yaml:"port"`
	HttpUsername                 string `yaml:"http_authorization_username"`
	HttpPassword                 string `yaml:"http_authorization_password"`
	StreamingBackupPort          string `yaml:"streaming_backup_port"`
	StreamingBackupHttpUsername  string `yaml:"streaming_backup_http_username"`
	StreamingBackupHttpPassword  string `yaml:"streaming_backup_http_password"`
	StreamingBackupSSLCommonName string `yaml:"streaming_backup_ssl_common_name"`
	StreamingBackupCACertPath    string `yaml:"streaming_backup_ca_cert_path"`
	SSLCommonName                string `yaml:"ssl_common_name"`
	SSLServerCertPath            string `yaml:"ssl_server_cert_path"`
	SSLServerKeyPath             string `yaml:"ssl_server_key_path"`
	SSLClientCertPath            string `yaml:"ssl_client_cert_path"`
	SSLClientKeyPath             string `yaml:"ssl_client_key_path"`
	SSLCACertPath                string `yaml:"ssl_ca_cert_path"`
	MySQLCACertPath              string `yaml:"mysql_ca_cert_path"`
}

func NewConfig(path string) (Config, error) {
	configContents, err := ioutil.ReadFile(path)
	if err != nil {
		return Config{}, errors.Wrap(err, "unable to find config file at path: "+path)
	}
	var cfg Config

	err = yaml.Unmarshal(configContents, &cfg)

	if err != nil {
		return Config{}, errors.Wrap(err, "unable to read config file")
	}

	return cfg, nil
}
