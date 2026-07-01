package fastrand_test

import (
	cryptorand "crypto/rand"
	"math/big"
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common/fastrand"
)

func TestBasicRandomness(t *testing.T) {
	arr := make([]int, 10)

	rng := fastrand.Rand{}
	for range len(arr) * 10000 {
		idx := rng.Intn(len(arr))
		arr[idx] = arr[idx] + 1
	}

	for _, value := range arr {
		assert.GreaterOrEqual(t, value, 10, "Expected to see at least 0.1 percent of items in each of 10 buckets")
	}
}

// ---
// For an extremely rough comparison, using the benchmarks in this package:
//                       │    sec/op    │
// LockedMathRand-18        52.20n ± 1%
// DisposableMathRand-18    1.473µ ± 1%
// CryptoRand-18            205.3n ± 1%
// FastRand-18             0.5052n ± 5%
//                       │      B/op      │
// LockedMathRand-18         0.000 ± 0%
// DisposableMathRand-18   5.250Ki ± 0%
// CryptoRand-18             48.00 ± 0%
// FastRand-18               0.000 ± 0%
//                       │  allocs/op   │
// LockedMathRand-18       0.000 ± 0%
// DisposableMathRand-18   1.000 ± 0%
// CryptoRand-18           3.000 ± 0%
// FastRand-18             0.000 ± 0%
// ---

func innerBenchmark(b *testing.B, rng func() int) {
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rng()
		}
	})
}

func BenchmarkLockedMathRand(b *testing.B) {
	var mu sync.Mutex
	rng := rand.New(rand.NewSource(42))
	rngFunc := func() int {
		mu.Lock()
		defer mu.Unlock()
		return rng.Int()
	}

	innerBenchmark(b, rngFunc)
}

func BenchmarkDisposableMathRand(b *testing.B) {
	rngFunc := func() int {
		return rand.New(rand.NewSource(42)).Int()
	}

	innerBenchmark(b, rngFunc)
}

func BenchmarkCryptoRand(b *testing.B) {
	max := big.NewInt(1000)
	rngFunc := func() int {
		a, _ := cryptorand.Int(cryptorand.Reader, max)
		return a.Sign() // This is techinically not returning the same thing as the others, but it seems rude to force this to go through a cast when we are comparing the source speed.
	}

	innerBenchmark(b, rngFunc)
}

func BenchmarkFastRand(b *testing.B) {
	innerBenchmark(b, fastrand.Rand{}.Int)
}
