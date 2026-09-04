//go:build aix || illumos || js || plan9 || wasip1

package localexecution

import (
	"errors"
	"os"
)

func tryLockBridgeState(*os.File) error {
	return errors.New("bridge state locking is not supported on this platform")
}

func unlockBridgeState(*os.File) error {
	return nil
}
