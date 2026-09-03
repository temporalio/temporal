//go:build windows

package mutationtest

import (
	"errors"
	"os"
	"os/exec"
	"strconv"
)

func configureCommandCancellation(cmd *exec.Cmd) {
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return os.ErrProcessDone
		}
		if err := exec.Command("taskkill", "/T", "/F", "/PID", strconv.Itoa(cmd.Process.Pid)).Run(); err == nil {
			return nil
		}
		err := cmd.Process.Kill()
		if errors.Is(err, os.ErrProcessDone) {
			return os.ErrProcessDone
		}
		return err
	}
}
