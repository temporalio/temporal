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

package common

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
	lock := NewRWMutex()
	err1 := lock.Lock(context.Background())
	s.Nil(err1)
	lock.Unlock()

	err2 := lock.RLock(context.Background())
	s.Nil(err2)
	lock.RUnlock()
}

func (s *LockSuite) TestExpiredContext() {
	lock := NewRWMutex()
	err1 := lock.Lock(context.Background())
	s.Nil(err1)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	cancel()
	err2 := lock.RLock(ctx)
	s.NotNil(err2)
	s.Equal(err2, ctx.Err())

	lock.Unlock()

	err3 := lock.Lock(context.Background())
	s.Nil(err3)
	lock.Unlock()
}
