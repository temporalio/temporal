package nettest

import "net"

// NewListener returns a net.Listener which uses the given Pipe to simulate a network connection.
func NewListener(pipe *Pipe) *PipeListener {
	return &PipeListener{
		Pipe: pipe,
		done: make(chan struct{}),
	}
}

// PipeListener is a net.Listener which uses a Pipe to simulate a network connection.
type PipeListener struct {
	*Pipe
	// We cancel calls to Accept using the done channel so that tests don't hang if they're broken.
	done chan struct{}
}

var _ net.Listener = (*PipeListener)(nil)

func (t *PipeListener) Accept() (net.Conn, error) {
	return t.Pipe.Accept(t.done)
}

func (t *PipeListener) Close() error {
	close(t.done)
	return nil
}

func (t *PipeListener) Addr() net.Addr {
	return &net.TCPAddr{}
}
