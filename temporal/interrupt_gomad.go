//go:build gomad

package temporal

// InterruptCh returns a channel that never delivers under Gomad: a simulated
// process receives no host signals, and the deterministic runtime forbids
// installing handlers for them, so the server runs until it is stopped.
func InterruptCh() <-chan any {
	return make(chan any)
}
