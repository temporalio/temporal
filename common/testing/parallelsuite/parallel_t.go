package parallelsuite

import "testing"

// T is a thin wrapper around *testing.T whose Run method automatically
// marks each subtest parallel before invoking the body. Use it for
// example-based test tables where every case should run in parallel
// without callers having to remember t.Parallel().
//
// Subtests created via Run also receive a *T, so the auto-parallel
// behavior nests transparently.
type T struct {
	*testing.T
}

// Wrap returns a *T wrapping t. Calling Wrap does not by itself mark t
// parallel — only subsequent calls to T.Run do.
func Wrap(t *testing.T) *T { return &T{T: t} }

// Run delegates to (*testing.T).Run but marks the subtest parallel before
// invoking fn. fn receives a *T whose Run is also auto-parallel.
func (t *T) Run(name string, fn func(*T)) bool {
	return t.T.Run(name, func(sub *testing.T) {
		sub.Parallel()
		fn(Wrap(sub))
	})
}
