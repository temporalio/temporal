package quotas

import (
	"testing"
	"time"
)

// BenchmarkRateLimiter
// BenchmarkRateLimiter-16           	 8445120	       125 ns/op
// BenchmarkDynamicRateLimiter
// BenchmarkDynamicRateLimiter-16    	 8629598	       132 ns/op

const (
	testRate  = 2000.0
	testBurst = 4000
)

func BenchmarkRateLimiter(b *testing.B) {
	limiter := NewRateLimiter(testRate, testBurst)
	for n := 0; n < b.N; n++ {
		limiter.Allow()
	}
}

func BenchmarkDynamicRateLimiter(b *testing.B) {
	limiter := NewDynamicRateLimiter(
		NewRateBurst(
			func() float64 { return testRate },
			func() int { return testBurst },
		),
		time.Minute,
	)
	for n := 0; n < b.N; n++ {
		limiter.Allow()
	}
}
