package jumpstart_test

import (
	"errors"

	"lf-agent/config"
	. "lf-agent/jumpstart"

	"io/ioutil"
	"os"

	"lf-agent/jumpstart/jumpstartfakes"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Jumpstart", func() {
	Describe("Prepare", func() {
		var (
			dir       string
			tmpFile   *os.File
			monitmock *jumpstartfakes.FakeMonitClient
			jumpstart *Jumpstart

			mysqlName = "mysql"
		)

		BeforeEach(func() {
			var err error
			monitmock = new(jumpstartfakes.FakeMonitClient)

			dir, err = ioutil.TempDir("", "fs_test_")
			Expect(err).NotTo(HaveOccurred())

			tmpFile, err = ioutil.TempFile(dir, "fs_test_")
			Expect(err).NotTo(HaveOccurred())

			jumpstart = NewJumpstart(
				nil,
				monitmock,
				nil,
				nil,
				config.Config{
					Datadir: dir,
				},
			)
		})

		AfterEach(func() {
			os.RemoveAll(dir)
		})

		It("stops the mysql process and deletes the data dir", func() {
			err := jumpstart.Prepare()
			Expect(err).NotTo(HaveOccurred())

			Expect(monitmock.StopCallCount()).To(Equal(1))
			Expect(monitmock.StopArgsForCall(0)).To(Equal(mysqlName))

			Expect(tmpFile.Name()).ToNot(BeAnExistingFile())
		})

		It("returns an error if the mysql process cannot be unmonitored", func() {
			monitmock.StopReturns(errors.New("some-error"))

			err := jumpstart.Prepare()
			Expect(err).To(MatchError("some-error"))
		})

		It("returns an error if the contents of mysql data dir failed to delete", func() {
			jumpstart.Cfg.Datadir = "/invalid/path"

			err := jumpstart.Prepare()
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("Perform", func() {
		var (
			monitmock  *jumpstartfakes.FakeMonitClient
			dbmock     *jumpstartfakes.FakeDatabaseClient
			backupmock *jumpstartfakes.FakeStreamingBackupClient
			execmock   *jumpstartfakes.FakeCmdRunner
			jumpstart  *Jumpstart

			mysqlName = "mysql"
		)

		BeforeEach(func() {
			execmock = new(jumpstartfakes.FakeCmdRunner)
			backupmock = new(jumpstartfakes.FakeStreamingBackupClient)
			monitmock = new(jumpstartfakes.FakeMonitClient)
			dbmock = new(jumpstartfakes.FakeDatabaseClient)

			jumpstart = NewJumpstart(
				dbmock,
				monitmock,
				execmock,
				backupmock,
				config.Config{
					Datadir: "/path/to/datadir",
				},
			)
		})

		It("Streams a backup from the leader", func() {
			err := jumpstart.Perform()
			Expect(err).NotTo(HaveOccurred())

			Expect(backupmock.StreamBackupCallCount()).To(Equal(1))

			Expect(execmock.RunCallCount()).To(Equal(1))
			Expect(execmock.RunArgsForCall(0).Args).To(Equal([]string{"/var/vcap/packages/xtrabackup/bin/xtrabackup", "--prepare", "--target-dir=/path/to/datadir"}))

			Expect(monitmock.StartCallCount()).To(Equal(1))
			Expect(monitmock.StartArgsForCall(0)).To(Equal(mysqlName))

			Expect(dbmock.ApplyGTIDPurgedCallCount()).To(Equal(1))
		})

		It("returns a error when reseting fails", func() {
			dbmock.ApplyGTIDPurgedReturns(errors.New("some-db-error"))

			err := jumpstart.Perform()
			Expect(err).To(MatchError("some-db-error"))
		})

		It("returns a error when streaming backup fails", func() {
			backupmock.StreamBackupReturns(errors.New("some-db-error"))

			err := jumpstart.Perform()
			Expect(err).To(MatchError("some-db-error"))
		})

		It("Returns an error when running xtrabackup fails", func() {
			execmock.RunReturnsOnCall(0, errors.New("some-exec-error"))

			err := jumpstart.Perform()
			Expect(err).To(MatchError("some-exec-error"))
		})

		It("returns a error when starting mysql fails", func() {
			monitmock.StartReturns(errors.New("some-db-error"))

			err := jumpstart.Perform()
			Expect(execmock.RunCallCount()).To(Equal(1))
			Expect(err).To(MatchError("some-db-error"))
		})
	})
})
