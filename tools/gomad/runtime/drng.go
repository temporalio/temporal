package sim_runtime

import "math/rand"

// drngSnapshot captures the state of a deterministic RNG for checkpoint/restore.
// Since math/rand.Rand does not expose its internal state, we track the seed
// and the number of Int63 calls made since seeding. On restore we re-create
// the Rand from the same seed and fast-forward by calling Int63 the recorded
// number of times.
type drngSnapshot struct {
	seed     int64
	callCount int64
}

// countingSource wraps a rand.Source64 and counts every Int63 call.
type countingSource struct {
	inner     rand.Source64
	callCount int64
}

func newCountingSource(seed int64) *countingSource {
	src := rand.NewSource(seed).(rand.Source64)
	return &countingSource{inner: src}
}

func (s *countingSource) Int63() int64 {
	s.callCount++
	return s.inner.Int63()
}

func (s *countingSource) Uint64() uint64 {
	s.callCount++
	return s.inner.Uint64()
}

func (s *countingSource) Seed(seed int64) {
	s.inner.Seed(seed)
	s.callCount = 0
}

// snapshot returns a drngSnapshot that can reconstruct the current DRNG state.
func (s *countingSource) snapshot() drngSnapshot {
	return drngSnapshot{
		seed:      0, // seed is stored externally on the simulator
		callCount: s.callCount,
	}
}

// newDrng creates a new *rand.Rand backed by a countingSource.
func newDrng(seed int64) (*rand.Rand, *countingSource) {
	src := newCountingSource(seed)
	return rand.New(src), src
}

// restoreDrng creates a new DRNG fast-forwarded to the given snapshot state.
func restoreDrng(seed int64, snap drngSnapshot) (*rand.Rand, *countingSource) {
	src := newCountingSource(seed)
	// fast-forward to the saved position
	for i := int64(0); i < snap.callCount; i++ {
		src.inner.Int63()
	}
	src.callCount = snap.callCount
	return rand.New(src), src
}
