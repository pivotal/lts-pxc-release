package agent_test

import (
	"fmt"
	"lf-agent/agent"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Status", func() {
	var status agent.DBStatus

	BeforeEach(func() {
		status = agent.DBStatus{
			IPAddress:             "10.0.0.1",
			ReadOnly:              true,
			ReplicationConfigured: true,
			ReplicationMode:       agent.ReplicationSemisync,
			GtidExecuted:          "some-guid-executed",
		}
	})

	It("presents instance info", func() {
		info := fmt.Sprintln(status)

		Expect(info).To(ContainSubstring("IP Address: 10.0.0.1"))
		Expect(info).To(ContainSubstring("Read Only: true"))
		Expect(info).To(ContainSubstring("Replication Configured: true"))
		Expect(info).To(ContainSubstring("Replication Mode: semisync"))
		Expect(info).To(ContainSubstring("GTID Executed: some-guid-executed"))
		Expect(info).To(ContainSubstring("Has Data: true"))
	})

	Context("when there's no data", func() {
		It("presents instance info", func() {
			status.GtidExecuted = ""

			info := fmt.Sprintln(status)

			Expect(info).To(ContainSubstring("Has Data: false"))
			Expect(info).To(ContainSubstring("GTID Executed: empty"))
		})
	})

	Context("when the replication topology has not been setup", func() {
		It("presents the role as 'unknown'", func() {
			status.ReadOnly = true
			status.ReplicationConfigured = false

			info := fmt.Sprintln(status)

			Expect(info).To(ContainSubstring("Role: unknown"))
		})
	})

	Context("When the instance is writable and replicaiton is not configured", func() {
		It("presents the role as 'leader'", func() {
			status.ReadOnly = false
			status.ReplicationConfigured = false

			info := fmt.Sprintln(status)

			Expect(info).To(ContainSubstring("Role: leader"))
		})
	})

	Context("When the instance is read-only and replicaiton is configured", func() {
		It("presents the role as 'follower'", func() {
			status.ReadOnly = true
			status.ReplicationConfigured = true

			info := fmt.Sprintln(status)

			Expect(info).To(ContainSubstring("Role: follower"))
		})
	})
})
