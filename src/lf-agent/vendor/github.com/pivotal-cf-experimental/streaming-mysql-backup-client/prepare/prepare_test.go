package prepare_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pivotal-cf-experimental/streaming-mysql-backup-client/prepare"
)

var _ = Describe("Prepare Command", func() {
	It("Uses innobackupex", func() {
		backupPrepare := prepare.DefaultBackupPreparer()

		cmd := backupPrepare.Command("path/to/backup")

		Expect(cmd.Path).To(ContainSubstring("innobackupex"))
		Expect(cmd.Args[1:]).To(Equal([]string{"--apply-log", "path/to/backup"}))
	})
})
