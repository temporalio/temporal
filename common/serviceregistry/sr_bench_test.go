// The MIT License
//
// Copyright (c) 2025 Temporal Technologies Inc.  All rights reserved.
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

package serviceregistry

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Dummy type for benchmarks
type foo struct {
	A, B int
}

// --- Benchmark for Instance strategy ---
func BenchmarkServiceRegistry_Get_Instance(b *testing.B) {
	sr := NewServiceRegistry()
	RegisterInstance(sr, &foo{A: 1, B: 2})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Get[*foo](sr)
	}
}

func BenchmarkServiceRegistry_Get_Instance_Parallel(b *testing.B) {
	sr := NewServiceRegistry()
	RegisterInstance(sr, &foo{A: 1, B: 2})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = Get[*foo](sr)
		}
	})
}

// --- Benchmark for Singleton strategy ---
func BenchmarkServiceRegistry_Get_Singleton(b *testing.B) {
	sr := NewServiceRegistry()
	err := RegisterSingleton[*foo](sr, func(_ *ServiceRegistry) (*foo, error) {
		return &foo{A: 1, B: 2}, nil
	})
	assert.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Get[*foo](sr)
	}
}

func BenchmarkServiceRegistry_Get_Singleton_Parallel(b *testing.B) {
	sr := NewServiceRegistry()
	err := RegisterSingleton[*foo](sr, func(_ *ServiceRegistry) (*foo, error) {
		return &foo{A: 1, B: 2}, nil
	})
	assert.NoError(b, err)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = Get[*foo](sr)
		}
	})
}

// --- Benchmark for Transient strategy ---
func BenchmarkServiceRegistry_Get_Transient(b *testing.B) {
	sr := NewServiceRegistry()
	err := RegisterTransient[*foo](sr, func(_ *ServiceRegistry, _ ...any) (*foo, error) {
		return &foo{A: 1, B: 2}, nil
	})
	assert.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Get[*foo](sr)
	}
}

func BenchmarkServiceRegistry_Get_Transient_Parallel(b *testing.B) {
	sr := NewServiceRegistry()
	err := RegisterTransient[*foo](sr, func(_ *ServiceRegistry, _ ...any) (*foo, error) {
		return &foo{A: 1, B: 2}, nil
	})
	assert.NoError(b, err)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = Get[*foo](sr)
		}
	})
}
