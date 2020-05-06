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

package pagination

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

var fetchMap = map[PageToken][]Entity{
	0: nil,
	1: {},
	2: {"one", "two", "three"},
	3: {"four", "five", "six", "seven"},
	4: {"eight"},
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
	fetchFn := func(token PageToken) (Page, error) {
		if token.(int) == 2 {
			return Page{
				CurrentToken: token,
				NextToken:    nil,
				Entities:     nil,
			}, nil
		}
		return Page{
			CurrentToken: token,
			NextToken:    token.(int) + 1,
			Entities:     fetchMap[token],
		}, nil
	}
	itr := NewIterator(0, fetchFn)
	s.False(itr.HasNext())
	_, err := itr.Next()
	s.Equal(ErrIteratorFinished, err)
}

func (s *IteratorSuite) TestNonEmptyNoErrors() {
	fetchFn := func(token PageToken) (Page, error) {
		var nextPageToken interface{} = token.(int) + 1
		if nextPageToken.(int) == 5 {
			nextPageToken = nil
		}
		return Page{
			CurrentToken: token,
			NextToken:    nextPageToken,
			Entities:     fetchMap[token],
		}, nil
	}
	itr := NewIterator(0, fetchFn)
	expectedResults := []string{"one", "two", "three", "four", "five", "six", "seven", "eight"}
	i := 0
	for itr.HasNext() {
		curr, err := itr.Next()
		s.NoError(err)
		s.Equal(expectedResults[i], curr.(string))
		i++
	}
	s.False(itr.HasNext())
	_, err := itr.Next()
	s.Equal(ErrIteratorFinished, err)
}

func (s *IteratorSuite) TestNonEmptyWithErrors() {
	fetchFn := func(token PageToken) (Page, error) {
		if token.(int) == 4 {
			return Page{}, errors.New("got error")
		}
		return Page{
			CurrentToken: token,
			NextToken:    token.(int) + 1,
			Entities:     fetchMap[token],
		}, nil
	}
	itr := NewIterator(0, fetchFn)
	expectedResults := []string{"one", "two", "three", "four", "five", "six", "seven"}
	i := 0
	for itr.HasNext() {
		curr, err := itr.Next()
		s.NoError(err)
		s.Equal(expectedResults[i], curr.(string))
		i++
	}
	s.False(itr.HasNext())
	curr, err := itr.Next()
	s.Nil(curr)
	s.Equal("got error", err.Error())
}
