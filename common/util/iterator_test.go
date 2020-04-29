// The MIT License (MIT)
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
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package util

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

var getMap = map[int][]byte{
	0: nil,
	1: {},
	2: []byte("\r\n\r\n\r\n"),
	3: []byte("\"one\"\r\n\"two\"\r\n"),
	4: []byte("\"three\"\r\n\"four\"\r\n\r\n\"five\"\r\n"),
	5: []byte("\r\n\"six\"\r\n\"seven\"\r\n\"eight\"\r\n"),
}

type IteratorSuite struct {
	*require.Assertions
	suite.Suite
}

func TestIteratorSuite(t *testing.T) {
	suite.Run(t, new(IteratorSuite))
}

func (s *IteratorSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *IteratorSuite) TestInitializedToEmpty() {
	getFn := func(page int) ([]byte, error) {
		return getMap[page], nil
	}
	itr := NewIterator(0, 2, getFn, []byte("\r\n"))
	s.False(itr.HasNext())
	_, err := itr.Next()
	s.Error(err)
}

func (s *IteratorSuite) TestNonEmptyNoErrors() {
	getFn := func(page int) ([]byte, error) {
		return getMap[page], nil
	}
	itr := NewIterator(0, 5, getFn, []byte("\r\n"))
	expectedResults := []string{"\"one\"", "\"two\"", "\"three\"", "\"four\"", "\"five\"", "\"six\"", "\"seven\"", "\"eight\""}
	i := 0
	for itr.HasNext() {
		curr, err := itr.Next()
		s.NoError(err)
		expectedCurr := []byte(expectedResults[i])
		s.Equal(expectedCurr, curr)
		i++
	}
	s.False(itr.HasNext())
	_, err := itr.Next()
	s.Error(err)
}

func (s *IteratorSuite) TestNonEmptyWithErrors() {
	getFn := func(page int) ([]byte, error) {
		if page > 4 {
			return nil, errors.New("error getting next page")
		}
		return getMap[page], nil
	}
	itr := NewIterator(0, 5, getFn, []byte("\r\n"))
	expectedResults := []string{"\"one\"", "\"two\"", "\"three\"", "\"four\"", "\"five\""}
	i := 0
	for itr.HasNext() {
		curr, err := itr.Next()
		s.NoError(err)
		expectedCurr := []byte(expectedResults[i])
		s.Equal(expectedCurr, curr)
		i++
	}
	s.False(itr.HasNext())
	_, err := itr.Next()
	s.Error(err)
}
