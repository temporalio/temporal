package testcore

import (
	"context"
	"errors"
	"sync"
)

// ErrGateReleased means the gate opened before enough calls arrived.
var ErrGateReleased = errors.New("gate is already released before enough calls arrived")

// Gate blocks operations until it is released.
// A released gate does not block new operations.
type Gate struct {
	mu sync.Mutex
	// Optionally allows users to inject an error that releasing returns
	// to propagate back to the waiters.
	err          error
	callsArrived int
	released     bool
	// callsArrivedCv acts like a condition variable to notify waiters
	// when callsArrived is incremented. Using a channel here allows us
	// to use it in a select in conjunction with context deadline.
	callsArrivedCv chan struct{}
	// Similarly, releaseCv is the equivalent condition variable that
	// releases all waiters. Only Release and ReleaseWithError close it.
	releaseCv chan struct{}
}

// NewGate returns a gate that blocks operations.
func NewGate() *Gate {
	return &Gate{
		callsArrivedCv: make(chan struct{}),
		releaseCv:      make(chan struct{}),
	}
}

// Arrive records the call and blocks until the gate is released or ctx is canceled.
func (g *Gate) Arrive(ctx context.Context) error {
	g.mu.Lock()
	if g.released {
		g.mu.Unlock()
		return nil
	}
	g.callsArrived++
	close(g.callsArrivedCv)
	g.callsArrivedCv = make(chan struct{})
	g.mu.Unlock()

	select {
	case <-g.releaseCv:
		g.mu.Lock()
		err := g.err
		g.mu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// NumArrived returns the number of calls that reached the closed gate.
func (g *Gate) NumArrived() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.callsArrived
}

// WaitForArrived waits for n calls, gate release, or context cancellation.
func (g *Gate) WaitForArrived(ctx context.Context, n int) error {
	for {
		g.mu.Lock()
		switch {
		case g.callsArrived >= n:
			g.mu.Unlock()
			return nil
		case g.released:
			g.mu.Unlock()
			return ErrGateReleased
		}
		callsArrivedCv := g.callsArrivedCv
		g.mu.Unlock()

		select {
		case <-callsArrivedCv:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Release opens the gate without an error.
func (g *Gate) Release() {
	g.doRelease(nil)
}

// ReleaseWithError opens the gate and returns err from each blocked callback.
func (g *Gate) ReleaseWithError(err error) {
	g.doRelease(err)
}

func (g *Gate) doRelease(err error) {
	g.mu.Lock()
	if g.released {
		g.mu.Unlock()
		return
	}
	g.err = err
	g.released = true
	close(g.releaseCv)
	close(g.callsArrivedCv)
	g.mu.Unlock()
}
