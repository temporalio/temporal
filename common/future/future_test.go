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
	require.False(s.T(), s.future.Ready())
	s.future.Set(s.value, s.err)

	value, err := s.future.Get(context.Background())
	require.True(s.T(), s.future.Ready())
	require.Equal(s.T(), s.value, value)
	require.Equal(s.T(), s.err, err)
}

func (s *futureSuite) TestSetGetReady_Parallel() {
	numGets := 1024

	startWG := sync.WaitGroup{}
	endWG := sync.WaitGroup{}

	startWG.Add(numGets)
	endWG.Add(numGets)

	require.False(s.T(), s.future.Ready())
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
			require.True(s.T(), s.future.Ready())
			require.Equal(s.T(), s.value, value)
			require.Equal(s.T(), s.err, err)
		}()
		startWG.Done()
	}

	endWG.Wait()
}

func (s *futureSuite) TestSetReadyGet_Sequential() {
	require.False(s.T(), s.future.Ready())
	s.future.Set(s.value, s.err)

	require.True(s.T(), s.future.Ready())
	value, err := s.future.Get(context.Background())
	require.Equal(s.T(), s.value, value)
	require.Equal(s.T(), s.err, err)
}

func (s *futureSuite) TestSetReadyGet_Parallel() {
	numGets := 1024

	startWG := sync.WaitGroup{}
	endWG := sync.WaitGroup{}

	startWG.Add(numGets)
	endWG.Add(numGets)

	require.False(s.T(), s.future.Ready())
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
			require.Equal(s.T(), s.value, value)
			require.Equal(s.T(), s.err, err)
		}()
		startWG.Done()
	}

	endWG.Wait()
}

func (s *futureSuite) TestGetWhenContextCanceled() {
	require.False(s.T(), s.future.Ready())
	s.future.Set(s.value, s.err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.True(s.T(), s.future.Ready())
	value, err := s.future.Get(ctx)
	require.NoError(s.T(), err, "When .Ready(), .Get() should return the value even if the context is canceled")
	require.Equal(s.T(), s.value, value)
}
