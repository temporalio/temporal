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

package backoff

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type (
	jitterSuite struct {
		suite.Suite
	}
)

func TestJitterSuite(t *testing.T) {
	s := new(jitterSuite)
	suite.Run(t, s)
}

func (s *jitterSuite) SetupSuite() {
}

func (s *jitterSuite) TestJitter_Int64() {
	input := int64(1048576)
	coefficient := float64(0.25)
	lowerBound := int64(float64(input) * (1 - coefficient))
	upperBound := int64(float64(input) * (1 + coefficient))
	fullJitterUpperBound := int64(float64(input) * 2)

	for i := 0; i < 1048576; i++ {
		result := Jitter(input, coefficient)
		s.True(result >= lowerBound)
		s.True(result < upperBound)

		result = FullJitter(input)
		s.True(result >= 0)
		s.True(result < fullJitterUpperBound)
	}
}

func (s *jitterSuite) TestJitter_Float64() {
	input := float64(1048576.1048576)
	coefficient := float64(0.16)
	lowerBound := float64(input) * (1 - coefficient)
	upperBound := float64(input) * (1 + coefficient)
	fullJitterUpperBound := float64(input) * 2

	for i := 0; i < 1048576; i++ {
		result := Jitter(input, coefficient)
		s.True(result >= lowerBound)
		s.True(result < upperBound)

		result = FullJitter(input)
		s.True(result >= 0)
		s.True(result < fullJitterUpperBound)
	}
}

func (s *jitterSuite) TestJitter_Duration() {
	input := time.Duration(1099511627776)
	coefficient := float64(0.1)
	lowerBound := time.Duration(int64(float64(input.Nanoseconds()) * (1 - coefficient)))
	upperBound := time.Duration(int64(float64(input.Nanoseconds()) * (1 + coefficient)))
	fullJitterUpperBound := time.Duration(int64(float64(input.Nanoseconds()) * 2))

	for i := 0; i < 1048576; i++ {
		result := Jitter(input, coefficient)
		s.True(result >= lowerBound)
		s.True(result < upperBound)

		result = FullJitter(input)
		s.True(result >= 0)
		s.True(result < fullJitterUpperBound)
	}
}

func (s *jitterSuite) TestJitter_InputZeroValue() {
	s.Zero(Jitter(time.Duration(0), rand.Float64()))
	s.Zero(Jitter(int64(0), rand.Float64()))
	s.Zero(Jitter(float64(0), rand.Float64()))
}

func (s *jitterSuite) TestJitter_CoeffientZeroValue() {
	s.Equal(time.Duration(1), Jitter(time.Duration(1), 0))
	s.Equal(int64(1), Jitter(int64(1), 0))
	s.Equal(float64(1), Jitter(float64(1), 0))
}
