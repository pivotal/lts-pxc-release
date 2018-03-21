package command_test

import (
	"errors"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"lf-agent/agent"
	"lf-agent/cmd/configure-leader-follower/command"
	fakes "lf-agent/cmd/configure-leader-follower/command/commandfakes"
)

var _ = Describe("Command Test", func() {
	Context("ElectLeaderCommand", func() {
		var (
			cmd    *command.ElectLeaderCommand
			agent0 *fakes.FakeLFClient
			agent1 *fakes.FakeLFClient
		)

		BeforeEach(func() {
			agent0 = new(fakes.FakeLFClient)
			agent0.HostnameReturns("mysql/0")
			agent1 = new(fakes.FakeLFClient)
			agent1.HostnameReturns("mysql/1")
			cmd = &command.ElectLeaderCommand{
				Instance0: agent0,
				Instance1: agent1,
			}
		})

		It("errors when the writable leader candidate has replication configured", func() {
			agent0.StatusReturns(&agent.DBStatus{
				ReadOnly:              false,
				ReplicationConfigured: true,
			}, nil)

			agent1.StatusReturns(&agent.DBStatus{
				ReadOnly:              true,
				ReplicationConfigured: true,
			}, nil)

			err := cmd.Execute()
			Expect(err).To(MatchError("Both instances are in an unexpected state. Replication is configured on the leader. For more information, see https://docs.pivotal.io/p-mysql"))
		})

		It("promotes first writable instance, when secondary instance is read-only replica", func() {
			agent0.StatusReturns(&agent.DBStatus{
				ReadOnly: false,
			}, nil)

			agent1.StatusReturns(&agent.DBStatus{
				ReadOnly:        true,
				ReplicationMode: agent.ReplicationAsync,
			}, nil)

			err := cmd.Execute()
			Expect(err).NotTo(HaveOccurred())

			Expect(agent0.MakeLeaderCallCount()).To(Equal(1))
			Expect(agent1.MakeFollowerCallCount()).To(Equal(1))
		})

		It("configures the follower first and then the leader", func() {
			agent0.StatusReturns(&agent.DBStatus{
				ReadOnly: false,
			}, nil)

			agent1.StatusReturns(&agent.DBStatus{
				ReadOnly:        true,
				ReplicationMode: agent.ReplicationAsync,
			}, nil)

			var followerMade bool
			agent0.MakeLeaderStub = func(failover bool) error {
				if !followerMade {
					return errors.New("Follower was not made first")
				}
				return nil
			}
			agent1.MakeFollowerStub = func() error {
				followerMade = true
				return nil
			}

			err := cmd.Execute()
			Expect(err).NotTo(HaveOccurred())

			Expect(agent0.MakeLeaderCallCount()).To(Equal(1))
			Expect(agent1.MakeFollowerCallCount()).To(Equal(1))
		})

		Context("when the second instance is writable and not configured for replication", func() {
			It("promotes the second instance when the first is a replica", func() {
				agent0.StatusReturns(&agent.DBStatus{
					ReadOnly:        true,
					ReplicationMode: agent.ReplicationAsync,
				}, nil)

				agent1.StatusReturns(&agent.DBStatus{
					ReadOnly: false,
				}, nil)

				err := cmd.Execute()
				Expect(err).NotTo(HaveOccurred())

				Expect(agent0.MakeFollowerCallCount()).To(Equal(1))
				Expect(agent1.MakeLeaderCallCount()).To(Equal(1))
			})
		})

		Context("when considering an initial deployment with no data", func() {
			It("promote the first instance and makes the second a follower", func() {
				agent0.StatusReturns(&agent.DBStatus{
					ReadOnly: true,
				}, nil)

				agent1.StatusReturns(&agent.DBStatus{
					ReadOnly: true,
				}, nil)

				err := cmd.Execute()
				Expect(err).NotTo(HaveOccurred())

				Expect(agent0.MakeLeaderCallCount()).To(Equal(1))
				Expect(agent1.MakeFollowerCallCount()).To(Equal(1))
			})
		})

		Context("when considering an existing deployment with data and no replication", func() {
			Context("when the first instance is writable, but the second is read-only", func() {
				It("promotes the first instance", func() {
					agent0.StatusReturns(&agent.DBStatus{
						ReadOnly:     false,
						GtidExecuted: "c698572c-d906-11e7-91a1-a691767534ea:1-10",
					}, nil)

					agent1.StatusReturns(&agent.DBStatus{
						ReadOnly:     true,
						GtidExecuted: "c698572c-d906-11e7-91a1-a691767534ea:1-10",
					}, nil)

					err := cmd.Execute()
					Expect(err).NotTo(HaveOccurred())

					Expect(agent0.MakeLeaderCallCount()).To(Equal(1))
					Expect(agent1.MakeFollowerCallCount()).To(Equal(1))
				})

				It("errors when the leader gtid executed is invalid", func() {
					agent0.StatusReturns(&agent.DBStatus{
						ReadOnly:     false,
						GtidExecuted: "some-invalid-gtid",
					}, nil)

					agent1.StatusReturns(&agent.DBStatus{
						ReadOnly:     true,
						GtidExecuted: "c698572c-d906-11e7-91a1-a691767534ea:1-10",
					}, nil)

					err := cmd.Execute()
					Expect(err.Error()).To(ContainSubstring("Unable to determine leader and follower. Error fetching GTIDs from leader:"))
				})

				It("errors when the follower gtid executed is invalid", func() {
					agent0.StatusReturns(&agent.DBStatus{
						ReadOnly:     false,
						GtidExecuted: "c698572c-d906-11e7-91a1-a691767534ea:1",
					}, nil)

					agent1.StatusReturns(&agent.DBStatus{
						ReadOnly:     true,
						GtidExecuted: "some-invalid-gtid",
					}, nil)

					err := cmd.Execute()
					Expect(err.Error()).To(ContainSubstring("Unable to determine leader and follower. Error fetching GTIDs from follower:"))
				})
			})

			Context("when a second instance is writable, but the first is read-only", func() {
				It("promotes the second instance", func() {
					agent0.StatusReturns(&agent.DBStatus{
						ReadOnly:     true,
						GtidExecuted: "c698572c-d906-11e7-91a1-a691767534ea:1-10",
					}, nil)

					agent1.StatusReturns(&agent.DBStatus{
						ReadOnly:     false,
						GtidExecuted: "c698572c-d906-11e7-91a1-a691767534ea:1-10",
					}, nil)

					err := cmd.Execute()
					Expect(err).NotTo(HaveOccurred())

					Expect(agent0.MakeFollowerCallCount()).To(Equal(1))
					Expect(agent1.MakeLeaderCallCount()).To(Equal(1))
				})
			})

			It("errors when the writable leader candidate's gtid_executed is not a superset of the follower", func() {
				agent0.StatusReturns(&agent.DBStatus{
					ReadOnly:     false,
					GtidExecuted: "c698572c-d906-11e7-91a1-a691767534ea:1",
				}, nil)

				agent1.StatusReturns(&agent.DBStatus{
					ReadOnly:        true,
					ReplicationMode: agent.ReplicationAsync,
					GtidExecuted:    "3e11fa47-71ca-11e1-9e33-c80aa9429562:1",
				}, nil)

				err := cmd.Execute()
				Expect(err).To(HaveOccurred())
				Expect(err).To(MatchError("Unable to determine leader and follower. Leader and follower data have diverged. For more information, see https://docs.pivotal.io/p-mysql"))
			})

			Context("when both instances are read-only", func() {
				It("promotes the instance with the gtid_executed superset", func() {
					agent0.StatusReturns(&agent.DBStatus{
						ReadOnly:     true,
						GtidExecuted: "c698572c-d906-11e7-91a1-a691767534ea:1-10",
					}, nil)

					agent1.StatusReturns(&agent.DBStatus{
						ReadOnly:     true,
						GtidExecuted: "c698572c-d906-11e7-91a1-a691767534ea:1-100",
					}, nil)

					err := cmd.Execute()
					Expect(err).NotTo(HaveOccurred())

					Expect(agent1.MakeLeaderCallCount()).To(Equal(1))
					Expect(agent0.MakeFollowerCallCount()).To(Equal(1))
				})

				It("chooses an instance when the instances' gtid_executed's are equal", func() {
					agent0.StatusReturns(&agent.DBStatus{
						ReadOnly:     true,
						GtidExecuted: "c698572c-d906-11e7-91a1-a691767534ea:1",
					}, nil)

					agent1.StatusReturns(&agent.DBStatus{
						ReadOnly:     true,
						GtidExecuted: "c698572c-d906-11e7-91a1-a691767534ea:1",
					}, nil)

					err := cmd.Execute()
					Expect(err).NotTo(HaveOccurred())

					Expect(agent0.MakeLeaderCallCount()).To(Equal(1))
					Expect(agent1.MakeFollowerCallCount()).To(Equal(1))
				})

				It("errors when the instances' gtid_executed's are not compatible", func() {
					agent0.StatusReturns(&agent.DBStatus{
						ReadOnly:     true,
						GtidExecuted: "c698572c-d906-11e7-91a1-a691767534ea:1-20",
					}, nil)

					agent1.StatusReturns(&agent.DBStatus{
						ReadOnly:     true,
						GtidExecuted: "c698572c-d906-11e7-91a1-a691767534ea:1-10,3e11fa47-71ca-11e1-9e33-c80aa9429562:1-10",
					}, nil)

					err := cmd.Execute()
					Expect(err).To(MatchError("Unable to determine leader and follower based on transaction history. For more information, see https://docs.pivotal.io/p-mysql"))
				})

				It("errors when the leader candidate based on gtid_executed has replication configured", func() {
					agent0.StatusReturns(&agent.DBStatus{
						ReadOnly:              true,
						ReplicationConfigured: true,
						GtidExecuted:          "c698572c-d906-11e7-91a1-a691767534ea:1-20",
					}, nil)

					agent1.StatusReturns(&agent.DBStatus{
						ReadOnly:              true,
						ReplicationConfigured: false,
						GtidExecuted:          "c698572c-d906-11e7-91a1-a691767534ea:1-10",
					}, nil)

					err := cmd.Execute()
					Expect(err).To(MatchError("Unable to determine leader and follower based on transaction history. For more information, see https://docs.pivotal.io/p-mysql"))
				})

				It("errors when an agent returns a bad gtid", func() {
					agent0.StatusReturns(&agent.DBStatus{
						ReadOnly:     true,
						GtidExecuted: "some-bad-gtid",
					}, nil)

					agent1.StatusReturns(&agent.DBStatus{
						ReadOnly:     true,
						GtidExecuted: "some-bad-gtid",
					}, nil)

					err := cmd.Execute()
					Expect(err).To(MatchError("Unable to determine leader and follower based on transaction history. For more information, see https://docs.pivotal.io/p-mysql"))

				})
			})
		})

		It("fails with an informative error when both instances are writable", func() {
			agent0.StatusReturns(&agent.DBStatus{
				ReadOnly: false,
			}, nil)

			agent1.StatusReturns(&agent.DBStatus{
				ReadOnly: false,
			}, nil)

			Expect(cmd.Execute()).To(MatchError("Both mysql instances are writable. Please ensure no divergent data and set one instance to read-only mode. For more information, see https://docs.pivotal.io/p-mysql"))
		})
	})

	Context("InspectCommand", func() {

		var (
			cmd        *command.InspectCommand
			localAgent *fakes.FakeLFClient
		)

		BeforeEach(func() {
			localAgent = new(fakes.FakeLFClient)
			localAgent.HostnameReturns("mysql/0")

			cmd = &command.InspectCommand{
				LocalClient: localAgent,
			}
		})
		It("successfully calls the status endpoint", func() {
			localAgent.StatusReturns(&agent.DBStatus{}, nil)
			err := cmd.Execute()

			Expect(err).NotTo(HaveOccurred())
			Expect(localAgent.StatusCallCount()).To(Equal(1))
		})

		It("returns an error when the request to the status endpoint fails", func() {
			localAgent.StatusReturns(nil, errors.New("yikes not responding"))

			err := cmd.Execute()
			Expect(err).To(MatchError("Failed to get status for mysql/0: yikes not responding"))
			Expect(localAgent.StatusCallCount()).To(Equal(1))
		})
	})
})
