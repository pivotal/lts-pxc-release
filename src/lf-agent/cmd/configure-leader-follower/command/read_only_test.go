package command_test

import (
	"errors"

	"lf-agent/cmd/configure-leader-follower/command"

	"lf-agent/cmd/configure-leader-follower/command/commandfakes"

	"lf-agent/agent"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("MakeReadOnly", func() {

	var (
		cmd          *command.MakeReadOnlyCommand
		localClient  *commandfakes.FakeLFClient
		remoteClient *commandfakes.FakeLFClient
	)

	BeforeEach(func() {
		localClient = new(commandfakes.FakeLFClient)
		localClient.HostnameReturns("mysql/0")
		remoteClient = new(commandfakes.FakeLFClient)
		remoteClient.HostnameReturns("mysql/1")
		cmd = &command.MakeReadOnlyCommand{
			LocalClient:  localClient,
			RemoteClient: remoteClient,
		}
	})

	It("calls read-only on the local agent and sync on the follower node", func() {
		localClient.StatusReturns(&agent.DBStatus{GtidExecuted: "some-gtid:1-10",}, nil)
		remoteClient.StatusReturns(&agent.DBStatus{ReplicationConfigured: true}, nil)
		err := cmd.Execute()
		Expect(err).NotTo(HaveOccurred())

		Expect(localClient.MakeReadOnlyCallCount()).To(Equal(1))
		Expect(remoteClient.SyncCallCount()).To(Equal(1))
		Expect(remoteClient.SyncArgsForCall(0)).To(Equal("some-gtid:1-10"))
	})

	It("returns an error when checking for peer status fails", func() {
		localClient.StatusReturns(&agent.DBStatus{GtidExecuted: "some-gtid:1-10"}, nil)
		remoteClient.StatusReturns(nil, errors.New("some-error on peer"))

		err := cmd.Execute()
		Expect(err).To(MatchError("failed to fetch status of peer (mysql/1): some-error on peer"))
	})

	It("returns an error when checking for local status fails", func() {
		remoteClient.StatusReturns(&agent.DBStatus{ReplicationConfigured: true}, nil)
		localClient.StatusReturns(nil, errors.New("some-error"))

		err := cmd.Execute()
		Expect(err).To(MatchError("failed to fetch status from local node: some-error"))
	})

	It("returns an error when make read only fails", func() {
		localClient.MakeReadOnlyReturns(errors.New("some-error"))

		err := cmd.Execute()
		Expect(err).To(MatchError("failed to set local node read-only: some-error"))
	})

	It("returns an error when sync fails", func() {
		localClient.StatusReturns(&agent.DBStatus{}, nil)
		remoteClient.StatusReturns(&agent.DBStatus{ReplicationConfigured: true}, nil)
		remoteClient.SyncReturns(errors.New("some-error"))

		err := cmd.Execute()
		Expect(err).To(MatchError("failed to sync transactions on peer: some-error"))
	})
})
