package server

import (
	"log"
	"os"

	"github.com/gorilla/mux"
	"github.com/unrolled/render"
	"github.com/urfave/negroni"

	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"lf-agent/config"
	"net/http"
)

type Server struct {
	Agent    Agent
	config   config.Config
	renderer *render.Render
}

func NewServer(lfAgent Agent, cfg config.Config) *Server {
	return &Server{
		Agent:    lfAgent,
		config:   cfg,
		renderer: render.New(),
	}
}

func (s *Server) Router() *mux.Router {
	router := mux.NewRouter()
	router.HandleFunc("/make-leader", s.makeLeaderHandler).Methods("POST")
	router.HandleFunc("/make-follower", s.makeFollowerHandler).Methods("POST")
	router.HandleFunc("/make-read-only", s.makeReadOnlyHandler).Methods("POST")
	router.HandleFunc("/status", s.statusHandler).Methods("GET")
	router.HandleFunc("/sync", s.syncHandler).Methods("POST")
	return router
}

func (s *Server) logger() *negroni.Logger {
	l := &negroni.Logger{
		ALogger: log.New(os.Stdout, "[api] ", 0),
	}
	l.SetDateFormat(negroni.LoggerDefaultDateFormat)
	l.SetFormat(negroni.LoggerDefaultFormat)
	return l
}

func (s *Server) Handler() *negroni.Negroni {
	logger := s.logger()
	n := negroni.New()
	authHandler := NewAuthHandler(s.renderer, s.config)

	n.Use(negroni.NewRecovery())
	n.Use(logger)
	n.Use(authHandler)

	n.UseHandler(s.Router())
	return n
}

func (s *Server) Serve() error {
	caCert, err := ioutil.ReadFile(s.config.SSLCACertPath)
	if err != nil {
		return err
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	tlsConfig := &tls.Config{
		MinVersion:               tls.VersionTLS12,
		PreferServerCipherSuites: true,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		},
		CurvePreferences: []tls.CurveID{
			tls.CurveP384,
		},
		ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs:  caCertPool,
	}

	tlsConfig.BuildNameToCertificate()

	server := &http.Server{
		Addr:      ":" + s.config.Port,
		Handler:   s.Handler(),
		TLSConfig: tlsConfig,
	}

	return server.ListenAndServeTLS(s.config.SSLServerCertPath, s.config.SSLServerKeyPath)
}
