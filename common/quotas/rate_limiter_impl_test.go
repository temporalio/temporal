// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	rateLimiterSuite struct {
		suite.Suite
		*require.Assertions
	}
)

func TestRateLimiterSuite(t *testing.T) {
	s := new(rateLimiterSuite)
	suite.Run(t, s)
}

func (s *rateLimiterSuite) SetupSuite() {

}

func (s *rateLimiterSuite) TearDownSuite() {

}

func (s *rateLimiterSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *rateLimiterSuite) TearDownTest() {

}

func (s *rateLimiterSuite) TestSetRate_Same() {
	rateLimiter := NewRateLimiter(testRate, testBurst)

	rateLimiterBefore := rateLimiter.goRateLimiter
	rateLimiter.SetRate(testRate)
	rateLimiterAfter := rateLimiter.goRateLimiter
	s.Equal(testRate, rateLimiter.Rate())
	s.Equal(testBurst, rateLimiter.Burst())
	s.Equal(rateLimiterBefore, rateLimiterAfter)
}

func (s *rateLimiterSuite) TestSetRate_Diff() {
	rateLimiter := NewRateLimiter(testRate, testBurst)

	newRate := testRate * 2
	rateLimiter.SetRate(newRate)
	s.Equal(newRate, rateLimiter.Rate())
	s.Equal(testBurst, rateLimiter.Burst())
}

func (s *rateLimiterSuite) TestSetBurst_Same() {
	rateLimiter := NewRateLimiter(testRate, testBurst)

	rateLimiterBefore := rateLimiter.goRateLimiter
	rateLimiter.SetBurst(testBurst)
	rateLimiterAfter := rateLimiter.goRateLimiter
	s.Equal(testRate, rateLimiter.Rate())
	s.Equal(testBurst, rateLimiter.Burst())
	s.Equal(rateLimiterBefore, rateLimiterAfter)
}

func (s *rateLimiterSuite) TestSetBurst_Diff() {
	rateLimiter := NewRateLimiter(testRate, testBurst)

	newBurst := testBurst * 2
	rateLimiter.SetBurst(newBurst)
	s.Equal(testRate, rateLimiter.Rate())
	s.Equal(newBurst, rateLimiter.Burst())
}

func (s *rateLimiterSuite) TestSetRateBurst_Same() {
	rateLimiter := NewRateLimiter(testRate, testBurst)

	rateLimiterBefore := rateLimiter.goRateLimiter
	rateLimiter.SetRateBurst(rateLimiter.Rate(), rateLimiter.Burst())
	rateLimiterAfter := rateLimiter.goRateLimiter
	s.Equal(testRate, rateLimiter.Rate())
	s.Equal(testBurst, rateLimiter.Burst())
	s.Equal(rateLimiterBefore, rateLimiterAfter)
}

func (s *rateLimiterSuite) TestSetRateBurst_Diff() {
	rateLimiter := NewRateLimiter(testRate, testBurst)

	newRate := rateLimiter.Rate() * 2
	newBurst := rateLimiter.Burst()
	rateLimiter.SetRateBurst(newRate, newBurst)
	s.Equal(newRate, rateLimiter.Rate())
	s.Equal(newBurst, rateLimiter.Burst())

	newRate = rateLimiter.Rate()
	newBurst = rateLimiter.Burst() * 2
	rateLimiter.SetRateBurst(newRate, newBurst)
	s.Equal(newRate, rateLimiter.Rate())
	s.Equal(newBurst, rateLimiter.Burst())

	newRate = rateLimiter.Rate() * 2
	newBurst = rateLimiter.Burst() * 2
	rateLimiter.SetRateBurst(newRate, newBurst)
	s.Equal(newRate, rateLimiter.Rate())
	s.Equal(newBurst, rateLimiter.Burst())
}
