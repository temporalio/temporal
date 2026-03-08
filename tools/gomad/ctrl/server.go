package sim_ctrl

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"

	"go.temporal.io/server/tools/gomad/util/verify"
)

type server struct {
	addr         string
	m            sync.Mutex
	listener     net.Listener
	shutdownCh   chan struct{}
	clientCh     chan struct{}
	connByClient map[string]net.Conn
	// msgByClient receives messages read from each client's TCP connection.
	// Key is client ID, value is a channel of message strings.
	msgByClient map[string]chan string
}

func newServer() *server {
	return &server{
		connByClient: map[string]net.Conn{},
		msgByClient:  map[string]chan string{},
		shutdownCh:   make(chan struct{}),
		clientCh:     make(chan struct{}),
		addr:         fmt.Sprintf("localhost:8888"),
	}
}

func (s *server) start() {
	fmt.Println("[ctrl]", "starting server at", s.addr)

	var err error
	s.listener, err = net.Listen("tcp", s.addr)
	if err != nil {
		panic(err)
	}

	go s.acceptConnections()
}

func (s *server) acceptConnections() {
	for {
		select {
		case <-s.shutdownCh:
			return
		default:
			conn, err := s.listener.Accept()
			if err != nil {
				continue
			}
			go s.handleConnection(conn)
		}
	}
}

func (s *server) handleConnection(conn net.Conn) {
	// listen for client id
	reader := bufio.NewReader(conn)
	clientId, err := reader.ReadString('\n')
	if err != nil {
		panic(err)
	}
	clientId = strings.TrimSpace(clientId)

	fmt.Println("[ctrl]", "connected client #"+clientId)

	msgCh := make(chan string, 64)

	s.m.Lock()
	verify.T(len(s.connByClient) < 2, "server already has 2 clients connected")
	s.connByClient[clientId] = conn
	s.msgByClient[clientId] = msgCh
	s.m.Unlock()

	// read messages from client over TCP
	go func() {
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			msgCh <- strings.TrimSpace(line)
		}
	}()

	s.clientCh <- struct{}{}
}

// clientMsgCh returns the message channel for the given client ID.
func (s *server) clientMsgCh(clientId string) chan string {
	s.m.Lock()
	defer s.m.Unlock()
	return s.msgByClient[clientId]
}

func (s *server) stop() {
	fmt.Println("[ctrl]", "stopping server")

	close(s.shutdownCh)
	err := s.listener.Close()
	if err != nil {
		fmt.Println("[ctrl]", "stopping server", err)
	}
}

func (s *server) sendAllClients(msg string) {
	verify.T(len(s.connByClient) == 2, "server needs 2 connected clients")

	for _, conn := range s.connByClient {
		_, err := conn.Write([]byte(fmt.Sprintf("%v\n", msg)))
		if err != nil {
			panic(err)
		}
	}
}
