package server_test

import (
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"

	"lf-agent/agent"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"

	"lf-agent/config"
	"lf-agent/server"
	"lf-agent/server/serverfakes"
	"strings"
)

var _ = Describe("Server", func() {
	Context("/make-leader", func() {
		var (
			s         *server.Server
			fakeAgent *serverfakes.FakeAgent
			rec       *httptest.ResponseRecorder
			body      io.ReadCloser
			cfg       config.Config
		)
		BeforeEach(func() {
			cfg = config.Config{
				HttpUsername: "test-user",
				HttpPassword: "test-password",
			}

			fakeAgent = &serverfakes.FakeAgent{}
			s = server.NewServer(fakeAgent, cfg)
			rec = httptest.NewRecorder()
			body = nil
		})

		It("Accepts a POST to /make-leader", func() {
			fakeAgent.CheckIfPromotableReturns(nil)
			req, err := http.NewRequest("POST", "/make-leader", strings.NewReader(`{"failover": false}`))
			Expect(err).NotTo(HaveOccurred())
			req.SetBasicAuth("test-user", "test-password")

			handler := s.Handler()
			handler.ServeHTTP(rec, req)

			Expect(rec.Code).To(Equal(http.StatusOK))
			body := gbytes.BufferWithBytes(rec.Body.Bytes())
			Expect(body).To(gbytes.Say(`{"status":"ok"}`))

			Expect(fakeAgent.MakeLeaderCallCount()).To(Equal(1))
			Expect(fakeAgent.CheckIfPromotableCallCount()).To(Equal(1))
			Expect(fakeAgent.ToggleHeartbeatsCallCount()).To(Equal(1))
		})

		It("Returns a 401 with bad authentication", func() {
			req, err := http.NewRequest("POST", "/make-leader", body)
			Expect(err).NotTo(HaveOccurred())
			req.SetBasicAuth("bad-user", "bad-password")

			handler := s.Handler()
			handler.ServeHTTP(rec, req)

			Expect(rec.Code).To(Equal(http.StatusUnauthorized))
			body := gbytes.BufferWithBytes(rec.Body.Bytes())
			Expect(body).To(gbytes.Say(`{"status":"Unauthorized"}`))

			Expect(fakeAgent.MakeLeaderCallCount()).To(BeZero())
			Expect(fakeAgent.CheckIfPromotableCallCount()).To(BeZero())
			Expect(fakeAgent.ToggleHeartbeatsCallCount()).To(BeZero())
		})

		It("Returns an error when /make-leader returns an error", func() {
			fakeAgent.CheckIfPromotableReturns(nil)
			fakeAgent.MakeLeaderReturns(errors.New("Promotion error"))

			req, err := http.NewRequest("POST", "/make-leader", strings.NewReader(`{"failover": false}`))
			Expect(err).NotTo(HaveOccurred())
			req.SetBasicAuth("test-user", "test-password")

			handler := s.Handler()
			handler.ServeHTTP(rec, req)

			Expect(rec.Code).To(Equal(http.StatusInternalServerError))
			body := gbytes.BufferWithBytes(rec.Body.Bytes())
			Expect(body).To(gbytes.Say(`{"status":"Promotion error"}`))

			Expect(fakeAgent.MakeLeaderCallCount()).To(Equal(1))
			Expect(fakeAgent.CheckIfPromotableCallCount()).To(Equal(1))
			Expect(fakeAgent.ToggleHeartbeatsCallCount()).To(BeZero())
		})

		It("Returns an error when /make-leader returns an error", func() {
			fakeAgent.CheckIfPromotableReturns(nil)

			req, err := http.NewRequest("POST", "/make-leader", strings.NewReader(""))
			Expect(err).NotTo(HaveOccurred())
			req.SetBasicAuth("test-user", "test-password")

			handler := s.Handler()
			handler.ServeHTTP(rec, req)

			Expect(rec.Code).To(Equal(http.StatusBadRequest))
			body := gbytes.BufferWithBytes(rec.Body.Bytes())
			Expect(body).To(gbytes.Say(`{"status":"failover must be specified"}`))

			Expect(fakeAgent.CheckIfPromotableCallCount()).To(BeZero())
			Expect(fakeAgent.MakeLeaderCallCount()).To(BeZero())
			Expect(fakeAgent.ToggleHeartbeatsCallCount()).To(BeZero())
		})

		It("Returns an error if checking for leader errors", func() {
			fakeAgent.CheckIfPromotableReturns(errors.New("Promotion error"))

			req, err := http.NewRequest("POST", "/make-leader", strings.NewReader(`{"failover": false}`))
			Expect(err).NotTo(HaveOccurred())
			req.SetBasicAuth("test-user", "test-password")

			handler := s.Handler()
			handler.ServeHTTP(rec, req)

			Expect(rec.Code).To(Equal(http.StatusInternalServerError))
			body := gbytes.BufferWithBytes(rec.Body.Bytes())
			Expect(body).To(gbytes.Say(`{"status":"Promotion error"}`))

			Expect(fakeAgent.MakeLeaderCallCount()).To(BeZero())
			Expect(fakeAgent.CheckIfPromotableCallCount()).To(Equal(1))
			Expect(fakeAgent.ToggleHeartbeatsCallCount()).To(BeZero())
		})
	})

	Context("/make-follower", func() {
		var (
			s         *server.Server
			fakeAgent *serverfakes.FakeAgent
			rec       *httptest.ResponseRecorder
			body      io.ReadCloser
			cfg       config.Config
		)

		BeforeEach(func() {
			cfg = config.Config{
				HttpUsername: "test-user",
				HttpPassword: "test-password",
			}

			fakeAgent = &serverfakes.FakeAgent{}
			s = server.NewServer(fakeAgent, cfg)
			rec = httptest.NewRecorder()
			body = nil
		})

		It("Accepts a POST to /make-follower", func() {
			req, err := http.NewRequest("POST", "/make-follower", body)
			Expect(err).NotTo(HaveOccurred())
			req.SetBasicAuth("test-user", "test-password")

			handler := s.Handler()
			handler.ServeHTTP(rec, req)

			Expect(rec.Code).To(Equal(http.StatusOK))
			body := gbytes.BufferWithBytes(rec.Body.Bytes())
			Expect(body).To(gbytes.Say(`{"status":"ok"}`))

			Expect(fakeAgent.MakeFollowerCallCount()).To(Equal(1))
		})

		It("Returns an error when MakeFollower returns an error", func() {
			fakeAgent.MakeFollowerReturns(errors.New("Some error"))

			req, err := http.NewRequest("POST", "/make-follower", body)
			Expect(err).NotTo(HaveOccurred())
			req.SetBasicAuth("test-user", "test-password")

			handler := s.Handler()
			handler.ServeHTTP(rec, req)

			Expect(rec.Code).To(Equal(http.StatusInternalServerError))
			body := gbytes.BufferWithBytes(rec.Body.Bytes())
			Expect(body).To(gbytes.Say(`{"status":"Some error"}`))
		})

		It("returns a 401 Unauthorized with bad authentication", func() {
			req, err := http.NewRequest("POST", "/make-follower", body)
			Expect(err).NotTo(HaveOccurred())
			req.SetBasicAuth("bad-user", "bad-password")

			handler := s.Handler()
			handler.ServeHTTP(rec, req)

			Expect(rec.Code).To(Equal(http.StatusUnauthorized))
			body := gbytes.BufferWithBytes(rec.Body.Bytes())
			Expect(body).To(gbytes.Say(`{"status":"Unauthorized"}`))

			Expect(fakeAgent.MakeFollowerCallCount()).To(BeZero())
		})
	})

	Context("POST /make-read-only", func() {
		var (
			s   *server.Server
			cfg config.Config
			rec *httptest.ResponseRecorder
			req *http.Request
			lfa *serverfakes.FakeAgent
		)
		BeforeEach(func() {
			cfg = config.Config{
				HttpUsername: "some-user",
				HttpPassword: "some-password",
			}
			lfa = &serverfakes.FakeAgent{}
			s = server.NewServer(lfa, cfg)
			var err error

			rec = httptest.NewRecorder()
			req, err = http.NewRequest("POST", "/make-read-only", nil)
			Expect(err).NotTo(HaveOccurred())
			req.SetBasicAuth("some-user", "some-password")
		})

		It("accepts a POST", func() {
			handler := s.Handler()
			handler.ServeHTTP(rec, req)

			Expect(rec.Code).To(Equal(http.StatusOK))
			Eventually(gbytes.BufferReader(rec.Body)).Should(gbytes.Say(`{"status":"ok"}`))

			Expect(lfa.MakeReadOnlyCallCount()).To(Equal(1))
		})

		It("returns an error when MakeReadOnlyFails", func() {
			lfa.MakeReadOnlyReturns(errors.New("read-only error"))

			handler := s.Handler()
			handler.ServeHTTP(rec, req)

			Expect(rec.Code).To(Equal(http.StatusInternalServerError))
			Eventually(gbytes.BufferReader(rec.Body)).Should(gbytes.Say(`{"status":"read-only error"}`))

			Expect(lfa.MakeReadOnlyCallCount()).To(Equal(1))
		})
	})

	Context("GET /status", func() {
		var (
			s   *server.Server
			cfg config.Config
			rec *httptest.ResponseRecorder
			req *http.Request
			lfa *serverfakes.FakeAgent
		)
		BeforeEach(func() {
			cfg = config.Config{
				HttpUsername: "some-user",
				HttpPassword: "some-password",
			}
			lfa = &serverfakes.FakeAgent{}
			s = server.NewServer(lfa, cfg)
			var err error

			rec = httptest.NewRecorder()
			req, err = http.NewRequest("GET", "/status", nil)
			Expect(err).NotTo(HaveOccurred())
			req.SetBasicAuth("some-user", "some-password")
		})

		It("accepts a GET", func() {
			status := &agent.DBStatus{
				IPAddress:             "some-ip",
				ReadOnly:              true,
				ReplicationConfigured: false,
				ReplicationMode:       agent.ReplicationAsync,
				GtidExecuted:          "abc:1-10",
			}
			lfa.MySQLStatusReturns(status, nil)

			handler := s.Handler()
			handler.ServeHTTP(rec, req)

			Expect(rec.Code).To(Equal(http.StatusOK))
			Expect(ioutil.ReadAll(rec.Body)).To(MatchJSON(`{"ip_address":"some-ip","read_only":true,"replication_configured": false,"replication_mode":0,"gtid_executed":"abc:1-10"}`))
			Expect(lfa.MySQLStatusCallCount()).To(Equal(1))
		})

		It("returns an error if MySQLStatusReturns fails", func() {
			lfa.MySQLStatusReturns(nil, errors.New("some-db-error"))

			handler := s.Handler()
			handler.ServeHTTP(rec, req)

			Expect(rec.Code).To(Equal(http.StatusInternalServerError))
			Expect(ioutil.ReadAll(rec.Body)).To(MatchJSON(`{"status":"some-db-error"}`))
			Expect(lfa.MySQLStatusCallCount()).To(Equal(1))
		})

		It("Returns a 401 with bad authentication", func() {
			req.SetBasicAuth("", "")
			handler := s.Handler()
			handler.ServeHTTP(rec, req)

			Expect(rec.Code).To(Equal(http.StatusUnauthorized))
			Expect(ioutil.ReadAll(rec.Body)).To(MatchJSON(`{"status":"Unauthorized"}`))
		})
	})

	Context("POST /sync", func() {
		var (
			s   *server.Server
			cfg config.Config
			rec *httptest.ResponseRecorder
			req *http.Request
			lfa *serverfakes.FakeAgent
		)
		BeforeEach(func() {
			cfg = config.Config{
				HttpUsername: "some-user",
				HttpPassword: "some-password",
			}
			lfa = &serverfakes.FakeAgent{}
			s = server.NewServer(lfa, cfg)
			var err error

			rec = httptest.NewRecorder()
			req, err = http.NewRequest("POST", "/sync", strings.NewReader(`{"peer_gtid_executed": "abc:1-5"}`))
			Expect(err).NotTo(HaveOccurred())
			req.SetBasicAuth("some-user", "some-password")
		})

		It("accepts a POST", func() {
			handler := s.Handler()
			handler.ServeHTTP(rec, req)

			Expect(rec.Code).To(Equal(http.StatusOK))
			Eventually(gbytes.BufferReader(rec.Body)).Should(gbytes.Say(`{"status":"ok"}`))

			Expect(lfa.SyncArgsForCall(0)).To(Equal("abc:1-5"))
			Expect(lfa.SyncCallCount()).To(Equal(1))
		})

		It("returns an error when peer_gtid_executed is empty", func() {
			lfa.SyncReturns(errors.New("demote error"))

			req.Body = nil

			handler := s.Handler()
			handler.ServeHTTP(rec, req)

			Expect(rec.Code).To(Equal(http.StatusBadRequest))
			Eventually(gbytes.BufferReader(rec.Body)).Should(gbytes.Say(`{"status":"peer gtid executed not provided"}`))

			Expect(lfa.SyncCallCount()).To(BeZero())
		})

		It("returns an error when Sync fails", func() {
			lfa.SyncReturns(errors.New("demote error"))

			handler := s.Handler()
			handler.ServeHTTP(rec, req)

			Expect(rec.Code).To(Equal(http.StatusInternalServerError))
			Eventually(gbytes.BufferReader(rec.Body)).Should(gbytes.Say(`{"status":"demote error"}`))

			Expect(lfa.SyncCallCount()).To(Equal(1))
		})
	})
})
