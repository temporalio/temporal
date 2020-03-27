// Copyright (c) 2019 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package quotas

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/time/rate"
)

const (
	defaultRps       = 2000
	defaultNamespace = "test"
	_minBurst        = 10000
)

func TestNewRateLimiter(t *testing.T) {
	maxDispatch := 0.01
	rl := NewRateLimiter(&maxDispatch, time.Second, _minBurst)
	limiter := rl.goRateLimiter.Load().(*rate.Limiter)
	assert.Equal(t, _minBurst, limiter.Burst())
}

func TestMultiStageRateLimiterBlockedByNamespaceRps(t *testing.T) {
	policy := newFixedRpsMultiStageRateLimiter(2, 1)
	var result []bool
	for n := 0; n < 5; n++ {
		result = append(result, policy.Allow(Info{Namespace: defaultNamespace}))
	}

	time.Sleep(time.Second)
	for n := 0; n < 5; n++ {
		result = append(result, policy.Allow(Info{Namespace: defaultNamespace}))
	}

	var numAllowed int
	for _, allowed := range result {
		if allowed {
			numAllowed++
		}
	}

	assert.Equal(t, 2, numAllowed)
}

func TestMultiStageRateLimiterBlockedByGlobalRps(t *testing.T) {
	policy := newFixedRpsMultiStageRateLimiter(1, 2)
	var result []bool
	for n := 0; n < 5; n++ {
		result = append(result, policy.Allow(Info{Namespace: defaultNamespace}))
	}

	time.Sleep(time.Second)
	for n := 0; n < 5; n++ {
		result = append(result, policy.Allow(Info{Namespace: defaultNamespace}))
	}

	var numAllowed int
	for _, allowed := range result {
		if allowed {
			numAllowed++
		}
	}

	assert.Equal(t, 2, numAllowed)
}

func BenchmarkRateLimiter(b *testing.B) {
	rps := float64(defaultRps)
	limiter := NewRateLimiter(&rps, 2*time.Minute, defaultRps)
	for n := 0; n < b.N; n++ {
		limiter.Allow()
	}
}

func BenchmarkMultiStageRateLimiter(b *testing.B) {
	policy := newFixedRpsMultiStageRateLimiter(defaultRps, defaultRps)
	for n := 0; n < b.N; n++ {
		policy.Allow(Info{Namespace: defaultNamespace})
	}
}

func BenchmarkMultiStageRateLimiter20Namespaces(b *testing.B) {
	numNamespaces := 20
	policy := newFixedRpsMultiStageRateLimiter(defaultRps, defaultRps)
	namespaces := getNamespaces(numNamespaces)
	for n := 0; n < b.N; n++ {
		policy.Allow(Info{Namespace: namespaces[n%numNamespaces]})
	}
}

func BenchmarkMultiStageRateLimiter100Namespaces(b *testing.B) {
	numNamespaces := 100
	policy := newFixedRpsMultiStageRateLimiter(defaultRps, defaultRps)
	namespaces := getNamespaces(numNamespaces)
	for n := 0; n < b.N; n++ {
		policy.Allow(Info{Namespace: namespaces[n%numNamespaces]})
	}
}

func BenchmarkMultiStageRateLimiter1000Namespaces(b *testing.B) {
	numNamespaces := 1000
	policy := newFixedRpsMultiStageRateLimiter(defaultRps, defaultRps)
	namespaces := getNamespaces(numNamespaces)
	for n := 0; n < b.N; n++ {
		policy.Allow(Info{Namespace: namespaces[n%numNamespaces]})
	}
}

func newFixedRpsMultiStageRateLimiter(globalRps, namespaceRps float64) Policy {
	return NewMultiStageRateLimiter(
		func() float64 {
			return globalRps
		},
		func(namespace string) float64 {
			return namespaceRps
		},
	)
}
func getNamespaces(n int) []string {
	namespaces := make([]string, n)
	for i := 0; i < n; i++ {
		namespaces = append(namespaces, fmt.Sprintf("namespaces%v", i))
	}
	return namespaces
}
