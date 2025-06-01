package aggregate

import (
	"math/rand"
	"testing"
	"time"
)

// BenchmarkArrayMovingWindowAvg
// BenchmarkArrayMovingWindowAvg-10    	17021074	        66.27 ns/op

const (
	testWindowSize = 10 * time.Millisecond
	testBufferSize = 200
)

func BenchmarkArrayMovingWindowAvg(b *testing.B) {
	avg := NewMovingWindowAvgImpl(testWindowSize, testBufferSize)
	for i := 0; i < b.N; i++ {
		avg.Record(rand.Int63())
		if i%10 == 0 {
			avg.Average()
		}
	}
}
