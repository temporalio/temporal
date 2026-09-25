package faultinjection

import (
	"context"
	"errors"
	"sync"
)

// ErrGateReleased means the gate opened before enough calls arrived.
var ErrGateReleased = errors.New("faultinjection: gate released before enough calls arrived")

// Gate blocks operations until it is released.
// A released gate does not block new operations.
type Gate struct {
	mu        sync.Mutex
	arrived   int
	changed   chan struct{}
	releaseCh chan struct{}
	err       error
	released  bool
}

// NewGate returns a gate that blocks operations.
func NewGate() *Gate {
	return &Gate{
		changed:   make(chan struct{}),
		releaseCh: make(chan struct{}),
	}
}

// Wait blocks until the gate is released or ctx is canceled.
func (g *Gate) Wait(ctx context.Context) error {
	g.mu.Lock()
	if g.released {
		g.mu.Unlock()
		return nil
	}
	g.arrived++
	close(g.changed)
	g.changed = make(chan struct{})
	g.mu.Unlock()

	select {
	case <-g.releaseCh:
		g.mu.Lock()
		err := g.err
		g.mu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Arrived returns the number of calls that reached the closed gate.
func (g *Gate) Arrived() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.arrived
}

// WaitForArrived waits for n calls, gate release, or context cancellation.
func (g *Gate) WaitForArrived(ctx context.Context, n int) error {
	for {
		g.mu.Lock()
		switch {
		case g.arrived >= n:
			g.mu.Unlock()
			return nil
		case g.released:
			g.mu.Unlock()
			return ErrGateReleased
		}
		changed := g.changed
		g.mu.Unlock()

		select {
		case <-changed:
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
	g.released = true
	g.err = err
	close(g.releaseCh)
	close(g.changed)
	g.mu.Unlock()
}
