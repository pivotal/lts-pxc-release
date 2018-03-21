package monit_test

import (
	"io/ioutil"
	. "lf-agent/monit"
	"net/http"
	"path/filepath"
	"time"

	"github.com/onsi/gomega/ghttp"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func Fixture(name string) []byte {
	path := filepath.Join("fixtures", name)
	Expect(path).To(BeAnExistingFile())
	contents, err := ioutil.ReadFile(path)
	Expect(err).NotTo(HaveOccurred())
	return contents
}

var _ = Describe("Monit", func() {
	var (
		monitClient *Client
		server      *ghttp.Server
	)

	BeforeEach(func() {
		server = ghttp.NewServer()
		monitClient = NewClient(server.URL(), "monit-user", "monit-password", 2*time.Second)
	})

	AfterEach(func() {
		server.Close()
	})

	Describe("start", func() {
		It("makes an start request to the monit API", func() {
			server.AppendHandlers(
				ghttp.CombineHandlers(
					ghttp.VerifyRequest(http.MethodPost, "/mysql"),
					ghttp.VerifyContentType("application/x-www-form-urlencoded"),
					ghttp.VerifyBasicAuth("monit-user", "monit-password"),
					ghttp.VerifyBody([]byte(`action=start`)),
					ghttp.RespondWith(http.StatusOK, nil),
				),
				ghttp.CombineHandlers(
					ghttp.VerifyRequest(http.MethodGet, "/_status", "format=xml"),
					ghttp.VerifyBasicAuth("monit-user", "monit-password"),
					ghttp.RespondWith(http.StatusOK, Fixture("started.xml")),
				),
			)

			err := monitClient.Start("mysql")
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns a timeout error when the service doesn't reach the desired state", func() {
			server.AppendHandlers(
				ghttp.CombineHandlers(
					ghttp.VerifyRequest(http.MethodPost, "/mysql"),
					ghttp.VerifyContentType("application/x-www-form-urlencoded"),
					ghttp.VerifyBasicAuth("monit-user", "monit-password"),
					ghttp.VerifyBody([]byte(`action=start`)),
					ghttp.RespondWith(http.StatusOK, nil),
				),
				ghttp.CombineHandlers(
					ghttp.VerifyRequest(http.MethodGet, "/_status", "format=xml"),
					ghttp.VerifyBasicAuth("monit-user", "monit-password"),
					ghttp.RespondWith(http.StatusOK, Fixture("stopped.xml")),
				),
			)

			err := monitClient.Start("mysql")
			Expect(err).To(MatchError("timed out waiting for mysql monit service to start: service status: 0, pending action: 0"))
		})

		It("returns a timeout error when the service has an ongoing action", func() {
			server.AppendHandlers(
				ghttp.CombineHandlers(
					ghttp.VerifyRequest(http.MethodPost, "/other-process"),
					ghttp.VerifyContentType("application/x-www-form-urlencoded"),
					ghttp.VerifyBasicAuth("monit-user", "monit-password"),
					ghttp.VerifyBody([]byte(`action=start`)),
					ghttp.RespondWith(http.StatusOK, nil),
				),
				ghttp.CombineHandlers(
					ghttp.VerifyRequest(http.MethodGet, "/_status", "format=xml"),
					ghttp.VerifyBasicAuth("monit-user", "monit-password"),
					ghttp.RespondWith(http.StatusOK, Fixture("stopped.xml")),
				),
			)

			err := monitClient.Start("other-process")
			Expect(err).To(MatchError("timed out waiting for other-process monit service to start: service status: 0, pending action: 6"))
		})

		It("returns a timeout error when the monit does not know about the service", func() {
			server.AppendHandlers(
				ghttp.CombineHandlers(
					ghttp.VerifyRequest(http.MethodPost, "/mysql"),
					ghttp.VerifyContentType("application/x-www-form-urlencoded"),
					ghttp.VerifyBasicAuth("monit-user", "monit-password"),
					ghttp.VerifyBody([]byte(`action=start`)),
					ghttp.RespondWith(http.StatusOK, nil),
				),
				ghttp.CombineHandlers(
					ghttp.VerifyRequest(http.MethodGet, "/_status", "format=xml"),
					ghttp.VerifyBasicAuth("monit-user", "monit-password"),
					ghttp.RespondWith(http.StatusOK, Fixture("missing.xml")),
				),
			)

			err := monitClient.Start("mysql")
			Expect(err).To(MatchError("timed out waiting for mysql monit service to start: service not found"))
		})

		It("returns a timeout error when the /_status code is not 200", func() {
			server.AppendHandlers(
				ghttp.CombineHandlers(
					ghttp.VerifyRequest(http.MethodPost, "/mysql"),
					ghttp.VerifyContentType("application/x-www-form-urlencoded"),
					ghttp.VerifyBasicAuth("monit-user", "monit-password"),
					ghttp.VerifyBody([]byte(`action=start`)),
					ghttp.RespondWith(http.StatusOK, nil),
				),
				ghttp.CombineHandlers(
					ghttp.VerifyRequest(http.MethodGet, "/_status", "format=xml"),
					ghttp.VerifyBasicAuth("monit-user", "monit-password"),
					ghttp.RespondWith(http.StatusInternalServerError, nil),
				),
			)

			err := monitClient.Start("mysql")
			Expect(err).To(MatchError("timed out waiting for mysql monit service to start: status code: 500"))
		})

		It("returns an error when the status code is not 200", func() {
			server.AppendHandlers(ghttp.CombineHandlers(
				ghttp.VerifyRequest(http.MethodPost, "/mysqlbad"),
				ghttp.VerifyContentType("application/x-www-form-urlencoded"),
				ghttp.VerifyBasicAuth("monit-user", "monit-password"),
				ghttp.VerifyBody([]byte(`action=start`)),
				ghttp.RespondWith(http.StatusNotFound, nil),
			))

			err := monitClient.Start("mysqlbad")
			Expect(err).To(MatchError("failed to make start request for mysqlbad: status code: 404"))
		})

		It("returns an error when it cannot make a request", func() {
			monitClient.URL = "some-bad-url"

			err := monitClient.Start("mysqlbad")
			Expect(err).To(MatchError(ContainSubstring("failed to make start request for mysqlbad:")))
		})
	})

	Describe("stop", func() {
		It("makes a stop request to the monit API", func() {
			server.AppendHandlers(
				ghttp.CombineHandlers(
					ghttp.VerifyRequest(http.MethodPost, "/mysql"),
					ghttp.VerifyContentType("application/x-www-form-urlencoded"),
					ghttp.VerifyBasicAuth("monit-user", "monit-password"),
					ghttp.VerifyBody([]byte(`action=stop`)),
					ghttp.RespondWith(http.StatusOK, nil),
				),
				ghttp.CombineHandlers(
					ghttp.VerifyRequest(http.MethodGet, "/_status", "format=xml"),
					ghttp.VerifyBasicAuth("monit-user", "monit-password"),
					ghttp.RespondWith(http.StatusOK, Fixture("stopped.xml")),
				),
			)

			err := monitClient.Stop("mysql")
			Expect(err).NotTo(HaveOccurred())
		})

		It("returns a timeout error when the service doesn't reach the desired state", func() {
			server.AppendHandlers(
				ghttp.CombineHandlers(
					ghttp.VerifyRequest(http.MethodPost, "/mysql"),
					ghttp.VerifyContentType("application/x-www-form-urlencoded"),
					ghttp.VerifyBasicAuth("monit-user", "monit-password"),
					ghttp.VerifyBody([]byte(`action=stop`)),
					ghttp.RespondWith(http.StatusOK, nil),
				),
				ghttp.CombineHandlers(
					ghttp.VerifyRequest(http.MethodGet, "/_status", "format=xml"),
					ghttp.VerifyBasicAuth("monit-user", "monit-password"),
					ghttp.RespondWith(http.StatusOK, Fixture("started.xml")),
				),
			)

			err := monitClient.Stop("mysql")
			Expect(err).To(MatchError("timed out waiting for mysql monit service to stop: service status: 1, pending action: 0"))
		})

		It("returns a timeout error when the service has an ongoing transaction", func() {
			server.AppendHandlers(
				ghttp.CombineHandlers(
					ghttp.VerifyRequest(http.MethodPost, "/other-process"),
					ghttp.VerifyContentType("application/x-www-form-urlencoded"),
					ghttp.VerifyBasicAuth("monit-user", "monit-password"),
					ghttp.VerifyBody([]byte(`action=stop`)),
					ghttp.RespondWith(http.StatusOK, nil),
				),
				ghttp.CombineHandlers(
					ghttp.VerifyRequest(http.MethodGet, "/_status", "format=xml"),
					ghttp.VerifyBasicAuth("monit-user", "monit-password"),
					ghttp.RespondWith(http.StatusOK, Fixture("started.xml")),
				),
			)

			err := monitClient.Stop("other-process")
			Expect(err).To(MatchError("timed out waiting for other-process monit service to stop: service status: 1, pending action: 6"))
		})

		It("returns a timeout error when the monit does not know about the service", func() {
			server.AppendHandlers(
				ghttp.CombineHandlers(
					ghttp.VerifyRequest(http.MethodPost, "/mysql"),
					ghttp.VerifyContentType("application/x-www-form-urlencoded"),
					ghttp.VerifyBasicAuth("monit-user", "monit-password"),
					ghttp.VerifyBody([]byte(`action=stop`)),
					ghttp.RespondWith(http.StatusOK, nil),
				),
				ghttp.CombineHandlers(
					ghttp.VerifyRequest(http.MethodGet, "/_status", "format=xml"),
					ghttp.VerifyBasicAuth("monit-user", "monit-password"),
					ghttp.RespondWith(http.StatusOK, Fixture("missing.xml")),
				),
			)

			err := monitClient.Stop("mysql")
			Expect(err).To(MatchError("timed out waiting for mysql monit service to stop: service not found"))
		})

		It("returns a timeout error when the /_status code is not 200", func() {
			server.AppendHandlers(
				ghttp.CombineHandlers(
					ghttp.VerifyRequest(http.MethodPost, "/mysql"),
					ghttp.VerifyContentType("application/x-www-form-urlencoded"),
					ghttp.VerifyBasicAuth("monit-user", "monit-password"),
					ghttp.VerifyBody([]byte(`action=stop`)),
					ghttp.RespondWith(http.StatusOK, nil),
				),
				ghttp.CombineHandlers(
					ghttp.VerifyRequest(http.MethodGet, "/_status", "format=xml"),
					ghttp.VerifyBasicAuth("monit-user", "monit-password"),
					ghttp.RespondWith(http.StatusInternalServerError, nil),
				),
			)

			err := monitClient.Stop("mysql")
			Expect(err).To(MatchError("timed out waiting for mysql monit service to stop: status code: 500"))
		})

		It("returns an error when the status code is not 200", func() {
			server.AppendHandlers(ghttp.CombineHandlers(
				ghttp.VerifyRequest(http.MethodPost, "/mysqlbad"),
				ghttp.VerifyContentType("application/x-www-form-urlencoded"),
				ghttp.VerifyBasicAuth("monit-user", "monit-password"),
				ghttp.VerifyBody([]byte(`action=stop`)),
				ghttp.RespondWith(http.StatusNotFound, nil),
			))

			err := monitClient.Stop("mysqlbad")
			Expect(err).To(MatchError("failed to make stop request for mysqlbad: status code: 404"))
		})

		It("returns an error when it cannot make a request", func() {
			monitClient.URL = "some-bad-url"

			err := monitClient.Stop("mysqlbad")
			Expect(err).To(MatchError(ContainSubstring("failed to make stop request for mysqlbad:")))
		})
	})
})
