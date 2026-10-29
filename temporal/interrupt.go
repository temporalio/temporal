package temporal

import (
	"os"
	"os/signal"
	"syscall"
)

func InterruptCh() <-chan any {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	ret := make(chan any, 1)
	go func() {
		s := <-c
		ret <- s
		close(ret)
	}()

	return ret
}
