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
