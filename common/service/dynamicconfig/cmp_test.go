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

package dynamicconfig

import (
	"math"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type compareEqualsTestSuite struct {
	suite.Suite
}

func TestCompareEqualsSuite(t *testing.T) {
	suite.Run(t, new(compareEqualsTestSuite))
}

func (s *compareEqualsTestSuite) TestIntCompareEquals() {
	s.True(intCompareEquals(0, 0))
	s.True(intCompareEquals(math.MaxInt32, math.MaxInt32))
	s.True(intCompareEquals(math.MinInt32, math.MinInt32))
	s.False(intCompareEquals(0, math.MaxInt32))
	s.False(intCompareEquals(math.MaxInt32, math.MinInt32))
}

func (s *compareEqualsTestSuite) TestFloat64CompareEquals() {
	s.True(float64CompareEquals(0.0, 0.0))
	s.True(float64CompareEquals(0.123456, 0.123456))
	s.True(float64CompareEquals(12345.123456, 12345.123456))
	s.False(float64CompareEquals(0.0, 0.1))
	s.False(float64CompareEquals(0.123456, 0.1234561))
	s.False(float64CompareEquals(12345.123456, 12345.1234567))
}

func (s *compareEqualsTestSuite) TestDurationCompareEquals() {
	s.True(durationCompareEquals(time.Second, time.Second))
	s.True(durationCompareEquals(time.Microsecond, time.Microsecond))
	s.True(durationCompareEquals(time.Millisecond, time.Millisecond))
	s.True(durationCompareEquals(time.Nanosecond, time.Nanosecond))
	s.True(durationCompareEquals(3600*time.Second, 3600*time.Second))
	s.False(durationCompareEquals(time.Nanosecond, time.Microsecond))
	s.False(durationCompareEquals(time.Microsecond+1, time.Microsecond))
}

func (s *compareEqualsTestSuite) TestMapCompareEquals() {
	buildMap := func() map[int]int {
		m := make(map[int]int)
		for i := 0; i < 10; i++ {
			m[rand.Int()] = rand.Int()
		}
		return m
	}

	cloneMap := func(src map[int]int) map[int]int {
		dst := make(map[int]int)
		for k, v := range src {
			dst[k] = v
		}
		return dst
	}

	for i := 0; i < 10; i++ {
		m := buildMap()
		s.True(reflect.DeepEqual(m, cloneMap(m)))
	}
}
