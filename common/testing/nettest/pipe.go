package nettest

import (
	"errors"
	"net"
)

type (
	// Pipe is an in-memory socket. It provides similar functionality to net.Listen and net.Dial via the Pipe.Accept
	// and Pipe.Connect methods, respectively. It is useful for faster and more deterministic testing of netcode.
	Pipe struct {
		clients chan connCb
	}

	// connResult captures a value which is either a connection or an error.
	connResult struct {
		conn net.Conn
		err  error
	}

	// connCb is a callback that a connection result will be sent to.
	connCb chan connResult
)

// ErrCanceled indicates that an operation was canceled by the client (e.g. via ctx.Done()).
var ErrCanceled = errors.New("pipe operation canceled by client")

// NewPipe returns a Pipe.
func NewPipe() *Pipe {
	return &Pipe{
		clients: make(chan connCb),
	}
}

// Accept returns a connection to the server for a client.
func (p *Pipe) Accept(cancel <-chan struct{}) (net.Conn, error) {
	select {
	case <-cancel:
		return nil, ErrCanceled
	case client := <-p.clients:
		clientConn, serverConn := net.Pipe()
		client <- connResult{conn: clientConn}

		return serverConn, nil
	}
}

// Connect returns a connection to a client for the server.
func (p *Pipe) Connect(cancel <-chan struct{}) (net.Conn, error) {
	clientConnCb := make(connCb)
	select {
	case p.clients <- clientConnCb:
		r := <-clientConnCb
		return r.conn, r.err
	case <-cancel:
		return nil, ErrCanceled
	}
}
