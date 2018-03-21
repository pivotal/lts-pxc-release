package jumpstart_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestJumpstart(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Jumpstart Suite")
}
