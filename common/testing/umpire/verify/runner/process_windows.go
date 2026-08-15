//go:build windows

package runner

import (
	"os/exec"
	"time"
)

func configureProcessCancellation(process *exec.Cmd) {
	process.WaitDelay = time.Second
}
