//go:build !(gomad && wasip1 && wasm)

package softassert

import "testing"

func TestSometimesEvaluatesArgumentOnce(t *testing.T) {
	calls := 0
	message := func() string { calls++; return "ordinary" }
	Sometimes(message())
	if calls != 1 {
		t.Fatalf("argument evaluated %d times", calls)
	}
}
