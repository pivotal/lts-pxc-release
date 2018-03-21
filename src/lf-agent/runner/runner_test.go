package runner_test

import (
	"io/ioutil"
	"os/exec"

	. "lf-agent/runner"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Runner", func() {
	Describe("Run", func() {

		var runner *Runner

		BeforeEach(func() {
			runner = &Runner{}
		})

		It("performs a command", func() {
			tempFile, err := ioutil.TempFile("", "mysql-test-")
			Expect(err).NotTo(HaveOccurred())

			cmd := exec.Command("rm", "-f", tempFile.Name())

			err = runner.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(tempFile.Name()).NotTo(BeAnExistingFile())
		})

		It("returns the error and args when there is an error", func() {
			cmd := exec.Command("bash", "-c", "echo -n some-error && exit 1")
			err := runner.Run(cmd)
			Expect(err).To(MatchError(ContainSubstring(`failed to execute 'bash -c echo -n some-error && exit 1': some-error`)))
		})
	})
})
