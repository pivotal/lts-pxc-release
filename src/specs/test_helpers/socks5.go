package test_helpers

import (
	"io/ioutil"
	"log"
	"net/url"

	"code.cloudfoundry.org/socks5-proxy"
	"github.com/pkg/errors"
)

func NewSocks5Dialer(proxyURL string, logger *log.Logger) (proxy.DialFunc, error) {
	u, err := url.Parse(proxyURL)
	if err != nil {
		return nil, err
	}

	if u.Scheme != "ssh+socks5" {
		return nil, errors.Errorf("Unexpected proxy schema %q", u.Scheme)
	}

	if len(u.Query()["private-key"]) == 0 {
		return nil, errors.New("no private key specified in proxy url")
	}

	if len(u.Query()["private-key"]) > 1 {
		return nil, errors.New("multiple private keys specified in proxy url")
	}

	privateKey := u.Query()["private-key"][0]
	userName := u.User.Username()

	sshKey, err := ioutil.ReadFile(privateKey)
	if err != nil {
		return nil, errors.Wrap(err, `failed to load private key from disk`)
	}

	socks5Proxy := proxy.NewSocks5Proxy(proxy.NewHostKey(), logger)

	return socks5Proxy.Dialer(userName, string(sshKey), u.Host)
}
