package bootstrap_test

import (
	"log"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/cloudfoundry/socks5-proxy"
	"github.com/go-sql-driver/mysql"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	helpers "specs/test_helpers"
)

func TestBootstrap(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "PXC Acceptance Tests -- Bootstrap Suite")
}

var (
	dialer     proxy.DialFunc
	httpClient *http.Client
)

var _ = BeforeSuite(func() {
	requiredEnvs := []string{
		"BOSH_ENVIRONMENT",
		"BOSH_CA_CERT",
		"BOSH_CLIENT",
		"BOSH_CLIENT_SECRET",
		"BOSH_GW_PRIVATE_KEY",
		"BOSH_GW_USER",
		"BOSH_DEPLOYMENT",
		"MYSQL_USERNAME",
		"MYSQL_PASSWORD",
		"GALERA_AGENT_USERNAME",
		"GALERA_AGENT_PASSWORD",
		"PROXY_USERNAME",
		"PROXY_PASSWORD",
	}
	helpers.CheckForRequiredEnvVars(requiredEnvs)

	if os.Getenv("BOSH_ALL_PROXY") != "" {
		var err error
		dialer, err = helpers.NewSocks5Dialer(
			os.Getenv("BOSH_ALL_PROXY"),
			log.New(GinkgoWriter, "[socks5proxy] ", log.LstdFlags),
		)
		Expect(err).NotTo(HaveOccurred())

		httpClient = &http.Client{
			Transport: &http.Transport{
				Dial: dialer,
			},
			Timeout: 5 * time.Second,
		}

		mysql.RegisterDial("tcp", func(addr string) (net.Conn, error) {
			return dialer("tcp", addr)
		})
	} else {
		httpClient = http.DefaultClient
	}
})
