package locks

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	LockSuite struct {
		*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
		suite.Suite
	}
)

func TestLockSuite(t *testing.T) {
	suite.Run(t, new(LockSuite))
}

func (s *LockSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
}

func (s *LockSuite) TestBasicLocking() {
	lock := NewMutex()
	err1 := lock.Lock(context.Background())
	s.Nil(err1)
	lock.Unlock()
}

func (s *LockSuite) TestExpiredContext() {
	lock := NewMutex()
	err1 := lock.Lock(context.Background())
	s.Nil(err1)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	cancel()
	err2 := lock.Lock(ctx)
	s.NotNil(err2)
	s.Equal(err2, ctx.Err())

	lock.Unlock()

	err3 := lock.Lock(context.Background())
	s.Nil(err3)
	lock.Unlock()
}

func BenchmarkLock(b *testing.B) {
	l := NewMutex()
	ctx := context.Background()
	for n := 0; n < b.N; n++ {
		l.Lock(ctx) //nolint:errcheck
		l.Unlock()  //nolint:staticcheck
	}
}
