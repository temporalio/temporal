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

package number

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	numberSuite struct {
		suite.Suite
		*require.Assertions
	}
)

func TestNumberSuite(t *testing.T) {
	s := new(numberSuite)
	suite.Run(t, s)
}

func (s *numberSuite) SetupSuite() {}

func (s *numberSuite) TearDownSuite() {}

func (s *numberSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *numberSuite) TearDownTest() {

}

func (s *numberSuite) TestInt() {
	number := rand.Intn(128)
	for _, n := range []interface{}{
		int8(number),
		int16(number),
		int32(number),
		int64(number),
		int(number),
	} {
		s.Equal(float64(number), NewNumber(n).GetFloatOrDefault(rand.Float64()))
		s.Equal(int(number), NewNumber(n).GetIntOrDefault(rand.Int()))
		s.Equal(uint(number), NewNumber(n).GetUintOrDefault(uint(rand.Uint64())))
	}
}

func (s *numberSuite) TestUint() {
	number := rand.Intn(256)
	for _, n := range []interface{}{
		uint8(number),
		uint16(number),
		uint32(number),
		uint64(number),
		uint(number),
	} {
		s.Equal(float64(number), NewNumber(n).GetFloatOrDefault(rand.Float64()))
		s.Equal(int(number), NewNumber(n).GetIntOrDefault(rand.Int()))
		s.Equal(uint(number), NewNumber(n).GetUintOrDefault(uint(rand.Uint64())))
	}
}

func (s *numberSuite) TestFloat() {
	number := rand.Float32() * float32(rand.Int())
	for _, n := range []interface{}{
		float32(number),
		float64(number),
	} {
		s.Equal(float64(number), NewNumber(n).GetFloatOrDefault(rand.Float64()))
		s.Equal(int(number), NewNumber(n).GetIntOrDefault(rand.Int()))
		s.Equal(uint(number), NewNumber(n).GetUintOrDefault(uint(rand.Uint64())))
	}
}
