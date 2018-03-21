package agent_test

import (
	"io/ioutil"
	"path/filepath"

	"lf-agent/agent"
	"lf-agent/config"

	"lf-agent/agent/agentfakes"

	"errors"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("SetReadOnly", func() {
	var (
		dbmock *agentfakes.FakeDatabaseClient
		lfa    *agent.LFAgent
		tmpDir string
	)

	BeforeEach(func() {
		var err error
		tmpDir, err = ioutil.TempDir("", "lf_read_only_")
		Expect(err).NotTo(HaveOccurred())

		dbmock = new(agentfakes.FakeDatabaseClient)

		lfa = agent.NewLFAgent(
			nil,
			dbmock,
			nil,
			config.Config{
				LfStateDir: tmpDir,
			},
		)
	})

	It("sets the db to super_read_only and removes the leader.cnf", func() {
		leaderCnfPath := filepath.Join(tmpDir, "leader.cnf")
		leaderContents := []byte("[mysqld]")
		err := ioutil.WriteFile(filepath.Join(tmpDir, "leader.cnf"), leaderContents, 0600)
		Expect(err).NotTo(HaveOccurred())

		Expect(
			lfa.MakeReadOnly(),
		).To(Succeed())

		Expect(dbmock.MakeReadOnlyCallCount()).To(Equal(1))
		Expect(leaderCnfPath).NotTo(BeAnExistingFile())
	})

	It("returns an error when db fails", func() {
		dbmock.MakeReadOnlyReturns(errors.New("some-error"))

		err := lfa.MakeReadOnly()
		Expect(err).To(MatchError("some-error"))
	})

	It("does not error if the leader.cnf file does not exist", func() {
		Expect(
			lfa.MakeReadOnly(),
		).To(Succeed())
	})
})
