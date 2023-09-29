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

package ndc

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	historypb "go.temporal.io/api/history/v1"
)

type (
	replicationTaskSuite struct {
		suite.Suite
		*require.Assertions
	}
)

func TestReplicationTaskSuite(t *testing.T) {
	s := new(replicationTaskSuite)
	suite.Run(t, s)
}

func (s *replicationTaskSuite) SetupSuite() {
	s.Assertions = require.New(s.T())
}

func (s *replicationTaskSuite) TearDownSuite() {

}

func (s *replicationTaskSuite) TestValidateEventsSlice() {
	eS1 := []*historypb.HistoryEvent{
		{
			EventId: 1,
			Version: 2,
		},
		{
			EventId: 2,
			Version: 2,
		},
	}
	eS2 := []*historypb.HistoryEvent{
		{
			EventId: 3,
			Version: 2,
		},
	}

	eS3 := []*historypb.HistoryEvent{
		{
			EventId: 4,
			Version: 2,
		},
	}

	v, err := validateEventsSlice(eS1, eS2)
	s.Equal(int64(2), v)
	s.Nil(err)

	v, err = validateEventsSlice(eS1, eS3)
	s.Equal(int64(0), v)
	s.IsType(ErrEventSlicesNotConsecutive, err)

	v, err = validateEventsSlice(eS1, nil)
	s.Equal(int64(0), v)
	s.IsType(ErrEmptyEventSlice, err)
}

func (s *replicationTaskSuite) TestValidateEvents() {
	eS1 := []*historypb.HistoryEvent{
		{
			EventId: 1,
			Version: 2,
		},
		{
			EventId: 2,
			Version: 2,
		},
	}

	eS2 := []*historypb.HistoryEvent{
		{
			EventId: 1,
			Version: 2,
		},
		{
			EventId: 3,
			Version: 2,
		},
	}

	eS3 := []*historypb.HistoryEvent{
		{
			EventId: 1,
			Version: 1,
		},
		{
			EventId: 2,
			Version: 2,
		},
	}

	v, err := validateEvents(eS1)
	s.Nil(err)
	s.Equal(int64(2), v)

	v, err = validateEvents(eS2)
	s.Equal(int64(0), v)
	s.IsType(ErrEventIDMismatch, err)

	v, err = validateEvents(eS3)
	s.Equal(int64(0), v)
	s.IsType(ErrEventVersionMismatch, err)
}
