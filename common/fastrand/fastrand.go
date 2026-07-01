// Package fastrand exposes an object [Rand] which can be used as a simple
// drop-in replacement for `[math/rand.Rand]` where performance or thread
// safety is required.
package fastrand

import (
	"math/rand"
	"sync"

	"github.com/caio/go-tdigest/v5"
)

// globalRngPool is a globally shared object for allowing lock-free reuse
// of shared random number generators. In practice we would not expect this
// pool to contain many more objects than the number of CPU cores running
// the code.
var globalRngPool = sync.Pool{
	New: func() any {
		return rand.New(rand.NewSource(rand.Int63()))
	},
}

// Rand is an object that behaves largely as-if it was a [math/rand.Rand],
// with the key distinction that it is thread-safe and highly performant.
//
// Under the hood this uses a thread-safe pool of [math/rand.Rand] objects
// which it will dynamically create and access for each call. As a result,
// this does not support setting the seed, since the underlying objects
// are ephemeral.
type Rand struct{}

// ExpFloat64 implements [math/rand.Rand.ExpFloat64].
func (r Rand) ExpFloat64() float64 {
	rng := globalRngPool.Get().(*rand.Rand)
	res := rng.ExpFloat64()
	globalRngPool.Put(rng)
	return res
}

// Float32 implements [math/rand.Rand.Float32].
func (r Rand) Float32() float32 {
	rng := globalRngPool.Get().(*rand.Rand)
	res := rng.Float32()
	globalRngPool.Put(rng)
	return res
}

// Float64 implements [math/rand.Rand.Float64].
func (r Rand) Float64() float64 {
	rng := globalRngPool.Get().(*rand.Rand)
	res := rng.Float64()
	globalRngPool.Put(rng)
	return res
}

// Int implements [math/rand.Rand.Int].
func (r Rand) Int() int {
	rng := globalRngPool.Get().(*rand.Rand)
	res := rng.Int()
	globalRngPool.Put(rng)
	return res
}

// Int31 implements [math/rand.Rand.Int31].
func (r Rand) Int31() int32 {
	rng := globalRngPool.Get().(*rand.Rand)
	res := rng.Int31()
	globalRngPool.Put(rng)
	return res
}

// Int31n implements [math/rand.Rand.Int31n].
func (r Rand) Int31n(n int32) int32 {
	rng := globalRngPool.Get().(*rand.Rand)
	res := rng.Int31n(n)
	globalRngPool.Put(rng)
	return res
}

// Int63 implements [math/rand.Rand.Int63].
func (r Rand) Int63() int64 {
	rng := globalRngPool.Get().(*rand.Rand)
	res := rng.Int63()
	globalRngPool.Put(rng)
	return res
}

// Int63n implements [math/rand.Rand.Int63n].
func (r Rand) Int63n(n int64) int64 {
	rng := globalRngPool.Get().(*rand.Rand)
	res := rng.Int63n(n)
	globalRngPool.Put(rng)
	return res
}

// Intn implements [math/rand.Rand.Intn].
func (r Rand) Intn(n int) int {
	rng := globalRngPool.Get().(*rand.Rand)
	res := rng.Intn(n)
	globalRngPool.Put(rng)
	return res
}

// NormFloat64 implements [math/rand.Rand.NormFloat64].
func (r Rand) NormFloat64() float64 {
	rng := globalRngPool.Get().(*rand.Rand)
	res := rng.NormFloat64()
	globalRngPool.Put(rng)
	return res
}

// Perm implements [math/rand.Rand.Perm].
func (r Rand) Perm(n int) []int {
	rng := globalRngPool.Get().(*rand.Rand)
	res := rng.Perm(n)
	globalRngPool.Put(rng)
	return res
}

// Read(p []byte)  implements [math/rand.Rand.Read(p []byte) ].
func (r Rand) Read(p []byte) (n int, err error) {
	rng := globalRngPool.Get().(*rand.Rand)
	res, err := rng.Read(p)
	globalRngPool.Put(rng)
	return res, err
}

// Seed implements [math/rand.Rand.Seed].
func (r Rand) Seed(seed int64) {
	// Do nothing, setting seeds is not supported since you may always get a different underlying rng.
}

// Shuffle(n int, swap func implements [math/rand.Rand.Shuffle(n int, swap func].
func (r Rand) Shuffle(n int, swap func(i int, j int)) {
	rng := globalRngPool.Get().(*rand.Rand)
	rng.Shuffle(n, swap)
	globalRngPool.Put(rng)
}

// Uint32 implements [math/rand.Rand.Uint32].
func (r Rand) Uint32() uint32 {
	rng := globalRngPool.Get().(*rand.Rand)
	res := rng.Uint32()
	globalRngPool.Put(rng)
	return res
}

// Uint64 implements [math/rand.Rand.Uint64].
func (r Rand) Uint64() uint64 {
	rng := globalRngPool.Get().(*rand.Rand)
	res := rng.Uint64()
	globalRngPool.Put(rng)
	return res
}

// Clone method is a no-op to support use in tdigest library.
func (r Rand) Clone() tdigest.RNG {
	return r
}
