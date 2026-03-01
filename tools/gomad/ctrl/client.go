package sim_ctrl

import (
	"fmt"
	"strings"

	"go.temporal.io/server/tools/gomad/runtime"
	"go.temporal.io/server/tools/gomad/util/verify"
)

const (
	logHistorySize = 30
)

type (
	simClient struct {
		conf    simConfig
		program *simProgram
		logs    []string
		paused  bool
	}
)

func newSimClient(
	conf simConfig,
	program *simProgram,
) *simClient {
	return &simClient{
		conf:    conf,
		program: program,
	}
}

func (c *simClient) start(pauseCh chan any) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("\n\n💥crash")
			c.printRecentLogs()
			panic(r)
		}
	}()

	statusCh, logsCh := c.program.start(c.conf)

loop:
	for {
		select {
		case line := <-logsCh:
			switch line {
			case sim_runtime.RemoteControlPauseCmd:
				verify.T(!c.paused, "⚠️client #"+c.conf.remoteId+" is already paused")
				fmt.Println("[ctrl]", "client #"+c.conf.remoteId+" paused now")
				c.paused = true
				pauseCh <- struct{}{}
			case sim_runtime.RemoteControlContinueCmd:
				verify.T(c.paused, "⚠️client #"+c.conf.remoteId+" is not paused")
				if len(c.logs) > logHistorySize {
					c.logs = c.logs[len(c.logs)-logHistorySize:] // trim logs to keep them manageable
				}
				c.paused = false
			default:
				verify.T(!c.paused, "⚠️client #"+c.conf.remoteId+" is paused but received log: %v", line)
				c.logs = append(c.logs, line)
			}

		case status := <-statusCh:
			verify.T(status.Error == nil, "⚠️client #"+c.conf.remoteId+" died: %v", status.Error)
			verify.T(status.Exit == 0, "⚠️client #"+c.conf.remoteId+" died: exit code %v", status.Exit)
			fmt.Println("[ctrl]", "client #"+c.conf.remoteId+" exited successfully")
			break loop
		}
	}
}

func (c *simClient) printRecentLogs() {
	fmt.Println("[ctrl]", "client #"+c.conf.remoteId+" recent log lines:", strings.Join(c.logs, "\n"))
}
