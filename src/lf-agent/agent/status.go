package agent

import (
	"fmt"
	"strings"
)

type DBStatus struct {
	IPAddress             string          `json:"ip_address"`
	ReadOnly              bool            `json:"read_only"`
	ReplicationConfigured bool            `json:"replication_configured"`
	ReplicationMode       ReplicationMode `json:"replication_mode"`
	GtidExecuted          string          `json:"gtid_executed"`
}

func (s *DBStatus) HasData() bool {
	return s.GtidExecuted != ""
}

func (s DBStatus) String() string {
	var gtidExecuted string
	var role = "unknown"

	if s.GtidExecuted == "" {
		gtidExecuted = "empty"
	} else {
		gtidExecuted = s.GtidExecuted
	}

	if s.ReadOnly && s.ReplicationConfigured {
		role = "follower"
	} else if !s.ReadOnly && !s.ReplicationConfigured {
		role = "leader"
	}

	args := []string{
		fmt.Sprintf("\nIP Address: %s", s.IPAddress),
		fmt.Sprintf("Role: %s", role),
		fmt.Sprintf("Read Only: %t", s.ReadOnly),
		fmt.Sprintf("Replication Configured: %t", s.ReplicationConfigured),
		fmt.Sprintf("Replication Mode: %s", s.ReplicationMode),
		fmt.Sprintf("Has Data: %t", s.HasData()),
		fmt.Sprintf("GTID Executed: %s", gtidExecuted),
	}

	return strings.Join(args, "\n")
}
