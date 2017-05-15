// Copyright (c) 2017 Uber Technologies, Inc.
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

package actor

import (
	"sync/atomic"
	"testing"
)

func BenchmarkCast(b *testing.B) {
	s := New("bench-cast-server")
	var cnt int64
	s.RegisterCast(0, func(interface{}) { cnt++ })
	s.Start()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s.Cast(0, nil)
	}

	<-s.Stop()
}

func BenchmarkCastParallel(b *testing.B) {
	s := New("bench-cast-server")
	var cnt int64
	s.RegisterCast(0, func(interface{}) { cnt++ })
	s.Start()

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s.Cast(0, nil)
		}
	})

	<-s.Stop()
}

func BenchmarkCall(b *testing.B) {
	s := New("bench-call-server")
	var cnt int64
	s.RegisterCall(0, func(interface{}) (interface{}, error) {
		cnt++
		return nil, nil
	})
	s.Start()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s.Call(0, nil)
	}

	<-s.Stop()
}

func BenchmarkCallParallel(b *testing.B) {
	s := New("bench-call-server")
	var cnt int64
	s.RegisterCall(0, func(interface{}) (interface{}, error) {
		cnt++
		return nil, nil
	})
	s.Start()

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s.Call(0, nil)
		}
	})

	<-s.Stop()
}

func BenchmarkBaseline(b *testing.B) {
	b.ReportAllocs()

	var cnt int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			atomic.AddInt64(&cnt, 1)
		}
	})
}
