package lf_client_test

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"net/http"
	"net/url"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/ghttp"

	"lf-agent/config"
	"lf-agent/lf_client"
)

var _ = Describe("LfClient", func() {

	var (
		lfClient *lf_client.Client
		server   *ghttp.Server

		lfClientFunc func() error
		httpMethod   string
		endPoint     string
		logTag       string
	)

	BeforeEach(func() {
		config := config.Config{
			SSLCommonName:     "lf-agent.dedicated-mysql.com",
			HttpUsername:      "test-user",
			HttpPassword:      "test-password",
			SSLClientCertPath: "./fixtures/client.crt",
			SSLClientKeyPath:  "./fixtures/client.key",
			SSLCACertPath:     "./fixtures/ca.crt",
		}
		caCert, err := ioutil.ReadFile(config.SSLCACertPath)
		Expect(err).NotTo(HaveOccurred())

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		cert, err := tls.LoadX509KeyPair("./fixtures/server.crt", "./fixtures/server.key")

		server = ghttp.NewUnstartedServer()
		server.HTTPTestServer.TLS = &tls.Config{
			MinVersion:               tls.VersionTLS12,
			PreferServerCipherSuites: true,
			CipherSuites: []uint16{
				tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			},
			CurvePreferences: []tls.CurveID{
				tls.CurveP384,
			},
			RootCAs:      caCertPool,
			Certificates: []tls.Certificate{cert},
			ServerName:   "lf-agent.dedicated-mysql.com",
		}
		server.HTTPTestServer.StartTLS()

		url, err := url.Parse(server.URL())
		Expect(err).NotTo(HaveOccurred())

		config.Port = url.Port()

		lfClient = lf_client.NewLfClient(url.Hostname(), config)
	})

	AfterEach(func() {
		server.Close()
	})

	shouldHandleAPIReqests := func() {
		It("returns an error when the request returns with a bad status", func() {
			server.AppendHandlers(ghttp.CombineHandlers(
				ghttp.VerifyRequest(httpMethod, endPoint),
				ghttp.VerifyBasicAuth("test-user", "test-password"),
				ghttp.RespondWith(http.StatusInternalServerError, `{"status":"some-error"}`),
			))

			Expect(lfClientFunc()).To(MatchError(logTag + ": [500 Internal Server Error] some-error"))
		})

		It("returns an error when the status code is unexpected", func() {
			server.AppendHandlers(ghttp.CombineHandlers(
				ghttp.VerifyRequest(httpMethod, endPoint),
				ghttp.VerifyBasicAuth("test-user", "test-password"),
				ghttp.RespondWith(http.StatusUnprocessableEntity, `{"status":"some-error"}`),
			))

			Expect(lfClientFunc()).To(MatchError(logTag + ": unexpected response from API"))
		})

		It("returns an error when authentication fails", func() {
			server.AppendHandlers(ghttp.CombineHandlers(
				ghttp.VerifyRequest(httpMethod, endPoint),
				ghttp.RespondWith(http.StatusUnauthorized, `{"status":"some-authorization-error"}`),
			))

			Expect(lfClientFunc()).To(MatchError(logTag + ": [401 Unauthorized] some-authorization-error"))
		})

		It("returns an error when the request is malformed", func() {
			server.AppendHandlers(ghttp.CombineHandlers(
				ghttp.VerifyRequest(httpMethod, endPoint),
				ghttp.RespondWith(http.StatusBadRequest, `{"status":"some-bad-request"}`),
			))

			Expect(lfClientFunc()).To(MatchError(logTag + ": [400 Bad Request] some-bad-request"))
		})

		It("returns an error when response body cannot be decoded", func() {
			server.AppendHandlers(ghttp.CombineHandlers(
				ghttp.VerifyRequest(httpMethod, endPoint),
				ghttp.RespondWith(http.StatusInternalServerError, `{status: not-ok}`),
			))

			Expect(lfClientFunc()).To(MatchError(ContainSubstring(logTag + ": failed to decode response:")))
		})
	}

	Describe("POST /make-leader", func() {
		BeforeEach(func() {
			lfClientFunc = func() error {
				return lfClient.MakeLeader(false)
			}

			httpMethod = http.MethodPost
			endPoint = "/make-leader"
			logTag = "make-leader request failed"
		})

		It("returns no error when the request succeeds", func() {
			server.AppendHandlers(ghttp.CombineHandlers(
				ghttp.VerifyRequest(http.MethodPost, endPoint),
				ghttp.VerifyBasicAuth("test-user", "test-password"),
				ghttp.RespondWith(http.StatusOK, `{"status":"ok"}`),
			))

			Expect(lfClientFunc()).To(Succeed())
		})

		shouldHandleAPIReqests()
	})

	Describe("POST /make-follower", func() {
		BeforeEach(func() {
			lfClientFunc = lfClient.MakeFollower
			httpMethod = http.MethodPost
			endPoint = "/make-follower"
			logTag = "make-follower request failed"
		})

		It("returns no error when the request succeeds", func() {
			server.AppendHandlers(ghttp.CombineHandlers(
				ghttp.VerifyRequest(http.MethodPost, endPoint),
				ghttp.VerifyBasicAuth("test-user", "test-password"),
				ghttp.RespondWith(http.StatusOK, `{"status":"ok"}`),
			))

			Expect(lfClientFunc()).To(Succeed())
		})

		shouldHandleAPIReqests()
	})

	Describe("POST /make-read-only", func() {
		BeforeEach(func() {
			lfClientFunc = lfClient.MakeReadOnly
			httpMethod = http.MethodPost
			endPoint = "/make-read-only"
			logTag = "make-read-only request failed"
		})

		It("returns no error when the request succeeds", func() {
			server.AppendHandlers(ghttp.CombineHandlers(
				ghttp.VerifyRequest(http.MethodPost, endPoint),
				ghttp.VerifyBasicAuth("test-user", "test-password"),
				ghttp.RespondWith(http.StatusOK, `{"status":"ok"}`),
			))

			Expect(lfClientFunc()).To(Succeed())
		})

		shouldHandleAPIReqests()
	})

	Describe("GET /status", func() {
		BeforeEach(func() {
			lfClientFunc = func() error {
				_, err := lfClient.Status()
				return err
			}

			httpMethod = http.MethodGet
			endPoint = "/status"
			logTag = "status request failed"
		})

		It("returns a DBStatus when the request succeeds", func() {
			server.AppendHandlers(ghttp.CombineHandlers(
				ghttp.VerifyRequest(http.MethodGet, "/status"),
				ghttp.VerifyBasicAuth("test-user", "test-password"),
				ghttp.RespondWith(http.StatusOK, `{"read_only":true}`),
			))

			status, err := lfClient.Status()
			Expect(err).NotTo(HaveOccurred())
			Expect(status.ReadOnly).To(BeTrue())
		})

		shouldHandleAPIReqests()
	})

	Describe("POST /sync", func() {
		BeforeEach(func() {
			lfClientFunc = func() error {
				return lfClient.Sync("some-gtid:1234")
			}

			httpMethod = http.MethodPost
			endPoint = "/sync"
			logTag = "sync request failed"
		})

		It("returns a DBStatus when the request succeeds", func() {
			server.AppendHandlers(ghttp.CombineHandlers(
				ghttp.VerifyRequest(http.MethodPost, endPoint),
				ghttp.VerifyJSON(`{"peer_gtid_executed" :"some-gtid:1234"}`),
				ghttp.VerifyBasicAuth("test-user", "test-password"),
				ghttp.RespondWith(http.StatusOK, `{"status":"ok"}`),
			))
			Expect(lfClientFunc()).To(Succeed())
		})

		shouldHandleAPIReqests()
	})
})
