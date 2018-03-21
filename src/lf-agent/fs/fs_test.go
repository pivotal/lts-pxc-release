package fs_test

import (
	"io/ioutil"
	"os"

	"lf-agent/fs"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Fs", func() {
	var (
		dir     string
		tmpFile *os.File
	)

	BeforeEach(func() {
		var err error
		dir, err = ioutil.TempDir("", "fs_test_")
		Expect(err).NotTo(HaveOccurred())

		tmpFile, err = ioutil.TempFile(dir, "fs_test_")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		os.RemoveAll(dir)
	})

	Context("CleanDirectory", func() {
		It("removes the contents of a directory", func() {
			err := fs.CleanDirectory(dir)
			Expect(err).NotTo(HaveOccurred())

			Expect(tmpFile.Name()).ToNot(BeAnExistingFile())
		})
	})
})
