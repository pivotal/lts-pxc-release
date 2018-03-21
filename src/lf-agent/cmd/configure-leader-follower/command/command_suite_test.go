package command_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"log"
	"os"
	"testing"
)

func TestCommand(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Command Suite")
}

var _ = BeforeSuite(func() {
	log.SetOutput(GinkgoWriter)
})

var _ = AfterSuite(func() {
	log.SetOutput(os.Stdout)
})
