package lf_client_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestLfClient(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "LfClient Suite")
}
