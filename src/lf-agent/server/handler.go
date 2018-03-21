package server

import (
	"encoding/json"
	"lf-agent/agent"
	"net/http"
)

//go:generate counterfeiter . Agent
type Agent interface {
	MakeFollower() error
	MakeLeader(failover bool) error
	CheckIfPromotable() error
	ToggleHeartbeats() error
	MakeReadOnly() error
	MySQLStatus() (*agent.DBStatus, error)
	Sync(peerGTIDExecuted string) error
}

type Response struct {
	Status string `json:"status"`
}

type SyncForm struct {
	PeerGTIDExecuted string `json:"peer_gtid_executed"`
}

type MakeLeaderArgs struct {
	Failover bool `json:"failover"`
}

func (s *Server) internalServerError(w http.ResponseWriter, resp *Response, err error) {
	resp.Status = err.Error()
	s.renderer.JSON(w, http.StatusInternalServerError, resp)
}

func (s *Server) makeLeaderHandler(w http.ResponseWriter, req *http.Request) {
	var (
		resp = &Response{}
		args = MakeLeaderArgs{}
	)

	if err := json.NewDecoder(req.Body).Decode(&args); err != nil {
		resp.Status = "failover must be specified"
		s.renderer.JSON(w, http.StatusBadRequest, resp)
		return
	}

	if err := s.Agent.CheckIfPromotable(); err != nil {
		s.internalServerError(w, resp, err)
		return
	}
	if err := s.Agent.MakeLeader(args.Failover); err != nil {
		s.internalServerError(w, resp, err)
		return
	}

	if err := s.Agent.ToggleHeartbeats(); err != nil {
		s.internalServerError(w, resp, err)
		return
	}

	resp.Status = "ok"
	s.renderer.JSON(w, http.StatusOK, resp)
}

func (s *Server) makeFollowerHandler(w http.ResponseWriter, req *http.Request) {
	resp := &Response{}

	if err := s.Agent.MakeFollower(); err != nil {
		s.internalServerError(w, resp, err)
		return
	}

	resp.Status = "ok"
	s.renderer.JSON(w, http.StatusOK, resp)
}

func (s *Server) makeReadOnlyHandler(w http.ResponseWriter, req *http.Request) {
	resp := &Response{}

	if err := s.Agent.MakeReadOnly(); err != nil {
		s.internalServerError(w, resp, err)
		return
	}

	resp.Status = "ok"
	s.renderer.JSON(w, http.StatusOK, resp)
}

func (s *Server) statusHandler(w http.ResponseWriter, req *http.Request) {
	resp := &Response{}
	status, err := s.Agent.MySQLStatus()

	if err != nil {
		s.internalServerError(w, resp, err)
		return
	}

	s.renderer.JSON(w, http.StatusOK, status)
}

func (s *Server) syncHandler(w http.ResponseWriter, req *http.Request) {
	var (
		resp = &Response{}
		form = SyncForm{}
	)

	if req.Body == nil {
		resp.Status = `peer gtid executed not provided`
		s.renderer.JSON(w, http.StatusBadRequest, resp)
		return
	}

	if err := json.NewDecoder(req.Body).Decode(&form); err != nil {
		s.internalServerError(w, resp, err)
		return
	}

	if err := s.Agent.Sync(form.PeerGTIDExecuted); err != nil {
		s.internalServerError(w, resp, err)
		return
	}

	resp.Status = "ok"
	s.renderer.JSON(w, http.StatusOK, resp)
}
