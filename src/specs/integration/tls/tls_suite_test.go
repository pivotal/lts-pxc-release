package tls_test

import (
	"database/sql"
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

var (
	mysqlConn *sql.DB
)

var _ = BeforeSuite(func() {
	helpers.SetupBoshDeployment()

	if os.Getenv("BOSH_ALL_PROXY") != "" {
		helpers.SetupSocks5Proxy()
	}

	mysqlUsername := "root"
	mysqlPassword, err := helpers.GetMySQLAdminPassword()
	Expect(err).NotTo(HaveOccurred())
	firstProxy, err := helpers.FirstProxyHost(helpers.BoshDeployment)
	Expect(err).NotTo(HaveOccurred())
	mysqlConn = helpers.DbConnWithUser(mysqlUsername, mysqlPassword, firstProxy)
})
