package testcore

import (
	"context"
	"errors"
	"sync"
)

// errGateReleased means the gate opened before enough calls arrived.
var errGateReleased = errors.New("gate is already released before enough calls arrived")

// gate blocks operations until it is released.
// A released gate does not block new operations.
type gate struct {
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
	// releases all waiters. Only release and releaseWithError close it.
	releaseCv chan struct{}
}

// newGate returns a gate that blocks operations.
func newGate() *gate {
	return &gate{
		callsArrivedCv: make(chan struct{}),
		releaseCv:      make(chan struct{}),
	}
}

// arrive records the call and blocks until the gate is released or ctx is canceled.
func (g *gate) arrive(ctx context.Context) error {
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

// numArrived returns the number of calls that reached the closed gate.
func (g *gate) numArrived() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.callsArrived
}

// waitForArrived waits for n calls, gate release, or context cancellation.
func (g *gate) waitForArrived(ctx context.Context, n int) error {
	for {
		g.mu.Lock()
		switch {
		case g.callsArrived >= n:
			g.mu.Unlock()
			return nil
		case g.released:
			g.mu.Unlock()
			return errGateReleased
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

// release opens the gate without an error.
func (g *gate) release() {
	g.doRelease(nil)
}

// releaseWithError opens the gate and returns err from each blocked callback.
func (g *gate) releaseWithError(err error) {
	g.doRelease(err)
}

func (g *gate) doRelease(err error) {
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
