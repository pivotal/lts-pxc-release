package lf_client

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/pkg/errors"

	"lf-agent/agent"
	"lf-agent/config"
	"lf-agent/server"
)

type Client struct {
	host string
	cfg  config.Config
}

func NewLfClient(host string, cfg config.Config) *Client {
	return &Client{
		host: host,
		cfg:  cfg,
	}
}

func (c *Client) getTLSConfig() (*tls.Config, error) {
	caCert, err := ioutil.ReadFile(c.cfg.SSLCACertPath)
	if err != nil {
		return nil, err
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	cert, err := tls.LoadX509KeyPair(c.cfg.SSLClientCertPath, c.cfg.SSLClientKeyPath)
	if err != nil {
		return nil, err
	}

	tlsConfig := &tls.Config{
		MinVersion:               tls.VersionTLS12,
		PreferServerCipherSuites: true,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		},
		CurvePreferences: []tls.CurveID{
			tls.CurveP384,
		},
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
	}

	tlsConfig.BuildNameToCertificate()
	tlsConfig.ServerName = c.cfg.SSLCommonName
	return tlsConfig, nil
}

func (c *Client) Hostname() string {
	return c.host
}

func (c *Client) Status() (*agent.DBStatus, error) {
	status := &agent.DBStatus{}
	if err := c.do(http.MethodGet, "status", nil, status); err != nil {
		return nil, errors.Wrapf(err, "status request failed")
	}

	return status, nil
}

func (c *Client) Sync(peerGtidExecuted string) error {
	syncForm := server.SyncForm{PeerGTIDExecuted: peerGtidExecuted}
	if err := c.do(http.MethodPost, "sync", &syncForm, nil); err != nil {
		return errors.Wrap(err, "sync request failed")
	}
	return nil
}

func (c *Client) MakeLeader(failover bool) error {
	args := server.MakeLeaderArgs{Failover: failover}
	if err := c.do(http.MethodPost, "make-leader", &args, nil); err != nil {
		return errors.Wrapf(err, "make-leader request failed")
	}

	return nil
}

func (c *Client) MakeFollower() error {
	if err := c.do(http.MethodPost, "make-follower", nil, nil); err != nil {
		return errors.Wrapf(err, "make-follower request failed")
	}

	return nil
}

func (c *Client) MakeReadOnly() error {
	if err := c.do(http.MethodPost, "make-read-only", nil, nil); err != nil {
		return errors.Wrapf(err, "make-read-only request failed")
	}

	return nil
}

func (c *Client) do(method, path string, body, dest interface{}) error {
	var requestBody io.Reader
	if body != nil {
		content, err := json.Marshal(body)
		if err != nil {
			return errors.Wrapf(err, "failed to encode request body")
		}
		requestBody = bytes.NewReader(content)
	}

	url := fmt.Sprintf("https://%s:%s/%s", c.host, c.cfg.Port, path)
	req, err := http.NewRequest(method, url, requestBody)
	if err != nil {
		return err
	}

	req.Header.Add("Content-Type", "application/json")

	req.SetBasicAuth(c.cfg.HttpUsername, c.cfg.HttpPassword)

	tlsConfig, err := c.getTLSConfig()
	if err != nil {
		return err
	}

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
	}

	response, err := client.Do(req)
	if err != nil {
		return err
	}

	switch response.StatusCode {
	case http.StatusOK:
		if dest != nil {
			if err := json.NewDecoder(response.Body).Decode(&dest); err != nil {
				return errors.Wrapf(err, "failed to decode response")
			}
		}
		return nil

	case http.StatusUnauthorized, http.StatusInternalServerError, http.StatusBadRequest:
		var body server.Response

		if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
			return errors.Wrapf(err, "failed to decode response")
		}

		return errors.Errorf("[%s] %s", response.Status, body.Status)
	default:
		return errors.New("unexpected response from API")
	}
}
