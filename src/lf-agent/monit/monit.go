package monit

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/html/charset"
)

const (
	MonitMonitorStatusStopped = 0
	MonitMonitorStatusStarted = 1
	CharsetEncoding           = "ISO-8859-1"
)

type Client struct {
	URL     string
	User    string
	Pass    string
	Timeout time.Duration
}

type MonitStatus struct {
	XMLName  xml.Name `xml:"monit"`
	Services []struct {
		XMLName       xml.Name `xml:"service"`
		Name          string   `xml:"name"`
		Monitor       int      `xml:"monitor"`
		PendingAction int      `xml:"pendingaction"`
	} `xml:"service"`
}

func NewClient(url, user, pass string, timeout time.Duration) *Client {
	return &Client{
		URL:     url,
		User:    user,
		Pass:    pass,
		Timeout: timeout,
	}
}

func (c *Client) Start(processName string) error {
	if _, err := c.do(http.MethodPost, "/"+processName, "action=start"); err != nil {
		return errors.Wrap(err, "failed to make start request for "+processName)
	}

	if err := c.waitForStatus(processName, MonitMonitorStatusStarted); err != nil {
		return errors.Wrapf(err, "timed out waiting for %s monit service to start", processName)
	}

	return nil
}

func (c *Client) Stop(processName string) error {
	if _, err := c.do(http.MethodPost, "/"+processName, "action=stop"); err != nil {
		return errors.Wrap(err, "failed to make stop request for "+processName)
	}

	if err := c.waitForStatus(processName, MonitMonitorStatusStopped); err != nil {
		return errors.Wrapf(err, "timed out waiting for %s monit service to stop", processName)
	}

	return nil
}

func (c *Client) waitForStatus(processName string, desiredStatus int) error {
	var err error
	timeoutChan := time.After(c.Timeout)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-timeoutChan:
			return err
		case <-ticker.C:
			var body io.ReadCloser
			body, err = c.do(http.MethodGet, "/_status", "", url.Values{"format": []string{"xml"}})
			if err != nil {
				continue
			}
			defer body.Close()

			var status MonitStatus
			decoder := xml.NewDecoder(body)
			decoder.CharsetReader = func(characterSet string, xmlReader io.Reader) (io.Reader, error) {
				return charset.NewReader(xmlReader, CharsetEncoding)
			}
			if err = decoder.Decode(&status); err != nil {
				continue
			}

			err = c.checkStatus(status, processName, desiredStatus)
			if err == nil {
				return nil
			}
		}
	}
}

func (c *Client) checkStatus(status MonitStatus, processName string, desiredStatus int) error {
	for _, service := range status.Services {
		if service.Name == processName {

			if service.Monitor == desiredStatus && service.PendingAction == 0 {
				return nil
			} else {
				return errors.Errorf("service status: %d, pending action: %d", service.Monitor, service.PendingAction)
			}
		}
	}

	return errors.New("service not found")
}

func (c *Client) do(method, path, reqBody string, queryParams ...url.Values) (io.ReadCloser, error) {
	body := strings.NewReader(reqBody)

	req, err := http.NewRequest(method, c.URL+path, body)
	if err != nil {
		return nil, err
	}

	req.SetBasicAuth(c.User, c.Pass)

	if len(queryParams) > 0 {
		req.URL.RawQuery = queryParams[0].Encode()
	}

	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}

	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	switch response.StatusCode {
	case http.StatusOK:
		return response.Body, nil
	default:
		return nil, errors.Errorf("status code: %d", response.StatusCode)
	}
}
