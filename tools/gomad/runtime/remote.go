package sim_runtime

import (
	"bufio"
	"fmt"
	"net"
	"strings"
)

type remoteCtrl struct {
	conn      net.Conn
	commandCh chan string
}

func newRemoteCtrl(addr, clientId string) *remoteCtrl {
	Dbg("🔌", "connecting to "+addr+" as #"+clientId)

	commandCh := make(chan string)

	// connect to remote controller
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		panic(err)
	}

	// identify to remote controller with client id
	_, err = conn.Write([]byte(fmt.Sprintf("%v\n", clientId)))
	if err != nil {
		panic(err)
	}

	// keep reading commands from remote controller
	go func() {
		for {
			reader := bufio.NewReader(conn)
			cmd, err := reader.ReadString('\n')
			if err != nil {
				panic(err)
			}
			commandCh <- strings.TrimSpace(cmd)
		}
	}()

	return &remoteCtrl{conn: conn, commandCh: commandCh}
}

func (h *remoteCtrl) hook(_ *Info) {
	// tell remote controller that client is waiting (via TCP, not stdout)
	_, err := h.conn.Write([]byte(RemoteControlPauseCmd + "\n"))
	if err != nil {
		panic(fmt.Sprintf("failed to send pause to controller: %v", err))
	}

	// wait for next command
	command := <-h.commandCh

	switch command {
	case RemoteControlContinueCmd:
		// acknowledge via TCP so controller can track state
		_, err := h.conn.Write([]byte(RemoteControlContinueCmd + "\n"))
		if err != nil {
			panic(fmt.Sprintf("failed to send continue ack to controller: %v", err))
		}
	default:
		panic(fmt.Sprintf("Unknown command: %v", command))
	}
}
