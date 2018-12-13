package bootstrap_test

import (
	"os"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	helpers "specs/test_helpers"
)

func TestBootstrap(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "PXC Acceptance Tests -- Bootstrap Suite")
}

var (
	galeraAgentUsername = "galera-agent"
	mysqlUsername       = "root"
	proxyUsername       = "proxy"
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
		"CREDHUB_SERVER",
		"CREDHUB_CLIENT",
		"CREDHUB_SECRET",
	}
	helpers.CheckForRequiredEnvVars(requiredEnvs)

	helpers.SetupBoshDeployment()

	if os.Getenv("BOSH_ALL_PROXY") != "" {
		helpers.SetupSocks5Proxy()
	}
})
