//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package localexecution

import (
	"os"

	"golang.org/x/sys/unix"
)

func tryLockBridgeState(file *os.File) error {
	return unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
}

func unlockBridgeState(file *os.File) error {
	return unix.Flock(int(file.Fd()), unix.LOCK_UN)
}
