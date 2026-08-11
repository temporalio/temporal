//go:build !darwin && !linux

package testcore

func processRSSBytes() uint64 {
	return 0
}
