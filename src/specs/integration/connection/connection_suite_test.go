package connection_test

import (
	"github.com/go-sql-driver/mysql"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"log"
	"net"
	"net/http"
	"testing"
	"time"

	helpers "specs/test_helpers"
)


func TestConnection(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "PXC Acceptance Tests -- Connection")
}

var (
	dialer              proxy.DialFunc
)
var _ = BeforeSuite(func() {
	requiredEnvs := []string{
		"MYSQL_HOST",
		"MYSQL_USERNAME",
		"MYSQL_PASSWORD",
	}
	helpers.CheckForRequiredEnvVars(requiredEnvs)
})

func setupSocks5Proxy() {
	log.Println("Setting up socks5 proxy")
	var err error
	dialer, err = helpers.NewSocks5Dialer(
		os.Getenv("BOSH_ALL_PROXY"),
		log.New(GinkgoWriter, "[socks5proxy] ", log.LstdFlags),
	)
	Expect(err).NotTo(HaveOccurred())

	mysql.RegisterDial("tcp", func(addr string) (net.Conn, error) {
		return dialer("tcp", addr)
	})
}
