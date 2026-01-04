package future

import (
	"context"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	futureSuite struct {
		*require.Assertions
		suite.Suite

		future *FutureImpl[int]
		value  int
		err    error
	}
)

func BenchmarkFutureAvailable(b *testing.B) {
	b.ReportAllocs()

	ctx := context.Background()
	futures := make([]*FutureImpl[interface{}], b.N)
	for n := 0; n < b.N; n++ {
		futures[n] = NewFuture[interface{}]()
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		future := futures[n]
		future.Set(nil, nil)
		_, _ = future.Get(ctx)
	}
}

func BenchmarkFutureGet(b *testing.B) {
	b.ReportAllocs()

	future := NewFuture[interface{}]()
	future.Set(nil, nil)
	ctx := context.Background()
	for n := 0; n < b.N; n++ {
		_, _ = future.Get(ctx)
	}
}

func BenchmarkFutureReady(b *testing.B) {
	b.ReportAllocs()

	future := NewFuture[interface{}]()
	future.Set(nil, nil)
	for n := 0; n < b.N; n++ {
		_ = future.Ready()
	}
}

func TestFutureSuite(t *testing.T) {
	s := new(futureSuite)
	suite.Run(t, s)
}

func (s *futureSuite) SetupSuite() {
	s.Assertions = require.New(s.T())
}

func (s *futureSuite) TearDownSuite() {

}

func (s *futureSuite) SetupTest() {
	s.future = NewFuture[int]()
	s.value = 123
	s.err = nil
}

func (s *futureSuite) TearDownTest() {

}

func (s *futureSuite) TestSetGetReady_Sequential() {
	s.False(s.future.Ready())
	s.future.Set(s.value, s.err)

	value, err := s.future.Get(context.Background())
	s.True(s.future.Ready())
	s.Equal(s.value, value)
	s.Equal(s.err, err)
}

func (s *futureSuite) TestSetGetReady_Parallel() {
	numGets := 1024

	startWG := sync.WaitGroup{}
	endWG := sync.WaitGroup{}

	startWG.Add(numGets)
	endWG.Add(numGets)

	s.False(s.future.Ready())
	go func() {
		startWG.Wait()
		s.future.Set(s.value, s.err)
	}()
	for i := 0; i < numGets; i++ {
		go func() {
			defer endWG.Done()

			ctx := context.Background()
			startWG.Wait()

			value, err := s.future.Get(ctx)
			s.True(s.future.Ready())
			s.Equal(s.value, value)
			s.Equal(s.err, err)
		}()
		startWG.Done()
	}

	endWG.Wait()
}

func (s *futureSuite) TestSetReadyGet_Sequential() {
	s.False(s.future.Ready())
	s.future.Set(s.value, s.err)

	s.True(s.future.Ready())
	value, err := s.future.Get(context.Background())
	s.Equal(s.value, value)
	s.Equal(s.err, err)
}

func (s *futureSuite) TestSetReadyGet_Parallel() {
	numGets := 1024

	startWG := sync.WaitGroup{}
	endWG := sync.WaitGroup{}

	startWG.Add(numGets)
	endWG.Add(numGets)

	s.False(s.future.Ready())
	go func() {
		startWG.Wait()
		s.future.Set(s.value, s.err)
	}()
	for i := 0; i < numGets; i++ {
		go func() {
			defer endWG.Done()

			ctx := context.Background()
			startWG.Wait()

			for !s.future.Ready() {
				runtime.Gosched()
			}

			value, err := s.future.Get(ctx)
			s.Equal(s.value, value)
			s.Equal(s.err, err)
		}()
		startWG.Done()
	}

	endWG.Wait()
}

func (s *futureSuite) TestGetWhenContextCanceled() {
	s.False(s.future.Ready())
	s.future.Set(s.value, s.err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s.True(s.future.Ready())
	value, err := s.future.Get(ctx)
	s.NoError(err, "When .Ready(), .Get() should return the value even if the context is canceled")
	s.Equal(s.value, value)
}

func (s *futureSuite) TestGetIfReady_Sequential() {
	s.False(s.future.Ready())
	_, err := s.future.GetIfReady()
	s.Equal(err, errorFutureNotReady)
	s.future.Set(s.value, s.err)
	value, err := s.future.GetIfReady()
	s.Equal(s.value, value)
	s.Equal(s.err, err)
}
