package agent_test

import (
	"errors"

	"lf-agent/agent"
	"lf-agent/config"

	"lf-agent/agent/agentfakes"

	"io/ioutil"

	"path/filepath"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("MakeFollower", func() {
	var (
		dbmock        *agentfakes.FakeDatabaseClient
		jumpstartmock *agentfakes.FakeJumpstart
		lfa           *agent.LFAgent
		tmpDir        string
	)

	BeforeEach(func() {
		dbmock = new(agentfakes.FakeDatabaseClient)
		jumpstartmock = new(agentfakes.FakeJumpstart)

		var err error
		tmpDir, err = ioutil.TempDir("", "lf_read_only_")
		Expect(err).NotTo(HaveOccurred())

		lfa = agent.NewLFAgent(nil, dbmock, jumpstartmock, config.Config{
			LfStateDir: tmpDir,
		})
	})

	It("calls MakeFollower", func() {
		dbmock.StatusReturns(&agent.DBStatus{GtidExecuted: "some-data"}, nil)

		Expect(lfa.MakeFollower()).To(Succeed())

		Expect(dbmock.StatusCallCount()).To(Equal(1))
		Expect(jumpstartmock.PrepareCallCount()).To(BeZero())
		Expect(jumpstartmock.PerformCallCount()).To(BeZero())

		Expect(dbmock.MakeReadOnlyCallCount()).To(Equal(1))
		Expect(dbmock.MakeFollowerCallCount()).To(Equal(1))
		Expect(dbmock.WaitForReplicationCallCount()).To(Equal(1))

		leaderCnfPath := filepath.Join(tmpDir, "leader.cnf")
		Expect(leaderCnfPath).NotTo(BeAnExistingFile())
	})

	It("runs jumpstart when the db has no data", func() {
		dbmock.StatusReturns(&agent.DBStatus{}, nil)

		Expect(lfa.MakeFollower()).To(Succeed())

		Expect(jumpstartmock.PrepareCallCount()).To(Equal(1))
		Expect(jumpstartmock.PerformCallCount()).To(Equal(1))
	})

	It("returns an error if preparing jumpstart fails", func() {
		dbmock.StatusReturns(&agent.DBStatus{}, nil)
		jumpstartmock.PrepareReturns(errors.New("some-error"))

		err := lfa.MakeFollower()
		Expect(err).To(MatchError("some-error"))

		Expect(jumpstartmock.PerformCallCount()).To(BeZero())
	})

	It("returns an error if performing jumpstart fails", func() {
		dbmock.StatusReturns(&agent.DBStatus{}, nil)
		jumpstartmock.PerformReturns(errors.New("some-error"))

		err := lfa.MakeFollower()
		Expect(err).To(MatchError("some-error"))

		Expect(jumpstartmock.PrepareCallCount()).To(Equal(1))
	})

	It("returns an error if getting DBStatus fails", func() {
		dbmock.StatusReturns(nil, errors.New("some db error"))

		err := lfa.MakeFollower()
		Expect(err).To(MatchError(`some db error`))
	})

	It("returns an error if MakeReadOnly fails", func() {
		dbmock.MakeReadOnlyReturns(errors.New("some db error"))

		err := lfa.MakeFollower()
		Expect(err).To(MatchError(`some db error`))
	})

	It("returns an error if MakeFollower fails", func() {
		dbmock.MakeFollowerReturns(errors.New("some db error"))
		dbmock.StatusReturns(&agent.DBStatus{}, nil)

		err := lfa.MakeFollower()
		Expect(err).To(MatchError(`some db error`))
	})

	It("returns an error if WaitForReplication fails", func() {
		dbmock.WaitForReplicationReturns(errors.New("some db error"))
		dbmock.StatusReturns(&agent.DBStatus{}, nil)

		err := lfa.MakeFollower()
		Expect(err).To(MatchError(`some db error`))
	})
})

var _ = Describe("Sync", func() {
	var (
		dbmock *agentfakes.FakeDatabaseClient
		lfa    *agent.LFAgent
	)

	BeforeEach(func() {
		dbmock = new(agentfakes.FakeDatabaseClient)
		lfa = agent.NewLFAgent(nil, dbmock, nil, config.Config{})
	})

	It("calls WaitForReceivedTransactions", func() {
		Expect(lfa.Sync("some-gtid:1234")).To(Succeed())
		Expect(dbmock.WaitForReceivedTransactionsCallCount()).To(Equal(1))
		Expect(dbmock.WaitForReceivedTransactionsArgsForCall(0)).To(Equal("some-gtid:1234"))
	})

	It("returns an error if WaitForReceivedTransactions fails", func() {
		dbmock.WaitForReceivedTransactionsReturns(errors.New("some db error"))
		err := lfa.Sync("some-gtid:1234")
		Expect(err).To(MatchError(`some db error`))
	})
})
