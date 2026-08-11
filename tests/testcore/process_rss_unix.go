//go:build darwin || linux

package testcore

import (
	"runtime"
	"syscall"
)

func processRSSBytes() uint64 {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0
	}
	rss := uint64(usage.Maxrss)
	if runtime.GOOS == "linux" {
		return rss * 1024
	}
	return rss
}
