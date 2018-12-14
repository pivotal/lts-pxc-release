package tls_test

import (
	"os"
	"testing"

	helpers "specs/test_helpers"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestTls(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Tls Suite")
}

var _ = BeforeSuite(func() {
	helpers.SetupBoshDeployment()

	if os.Getenv("BOSH_ALL_PROXY") != "" {
		helpers.SetupSocks5Proxy()
	}
})
