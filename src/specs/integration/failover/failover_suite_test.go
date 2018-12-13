package failover_test

import (
	"os"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	helpers "specs/test_helpers"
)

func TestFailover(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "PXC Acceptance Tests -- Failover")
}

var _ = BeforeSuite(func() {
	requiredEnvs := []string{
		"BOSH_ENVIRONMENT",
		"BOSH_CA_CERT",
		"BOSH_CLIENT",
		"BOSH_CLIENT_SECRET",
		"BOSH_DEPLOYMENT",
		"MYSQL_USERNAME",
		"MYSQL_PASSWORD",
		"PROXY_USERNAME",
		"PROXY_PASSWORD",
	}
	helpers.CheckForRequiredEnvVars(requiredEnvs)

	if os.Getenv("BOSH_ALL_PROXY") != "" {
		helpers.SetupSocks5Proxy()
	}
})
