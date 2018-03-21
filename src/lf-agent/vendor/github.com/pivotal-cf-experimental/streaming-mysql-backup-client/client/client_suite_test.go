package client_test

import (
	"testing"

	"code.cloudfoundry.org/lager"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestMysqlBackupInitiator(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Streaming MySQL Backup Client Suite")
}

var logger lager.Logger

var _ = BeforeSuite(func() {
	logger = lager.NewLogger("backup-client-test")
})

var _ = AfterSuite(func() {
})
