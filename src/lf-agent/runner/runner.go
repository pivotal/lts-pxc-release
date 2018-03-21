package runner

import (
	"fmt"
	"os/exec"
	"strings"

	"github.com/pkg/errors"
)

type Runner struct{}

func (r Runner) Run(cmd *exec.Cmd) error {
	output, err := cmd.CombinedOutput()
	if err != nil {
		cmdArgsAndOutput := fmt.Sprintf("failed to execute '%s': %s", strings.Join(cmd.Args, " "), output)
		return errors.Wrap(err, cmdArgsAndOutput)
	}

	return nil
}
