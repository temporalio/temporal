//go:build !aix && !android && !darwin && !dragonfly && !freebsd && !hurd && !illumos && !ios && !linux && !netbsd && !openbsd && !solaris && !windows

package mutationtest

import "os/exec"

func configureCommandCancellation(_ *exec.Cmd) {
}
