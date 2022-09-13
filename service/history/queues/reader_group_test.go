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

package queues

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common"
)

type (
	readerGroupSuite struct {
		suite.Suite
		*require.Assertions

		readerGroup *ReaderGroup
	}

	testReader struct {
		status int32
	}
)

func TestReaderGroupSuite(t *testing.T) {
	s := new(readerGroupSuite)
	suite.Run(t, s)
}

func (s *readerGroupSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.readerGroup = NewReaderGroup(func(_ int32, _ []Slice) Reader {
		return newTestReader()
	})
}

func (s *readerGroupSuite) TestStartStop() {
	r := s.readerGroup.NewReader(DefaultReaderId)
	s.Equal(common.DaemonStatusInitialized, r.(*testReader).status)

	s.readerGroup.Start()
	s.Equal(common.DaemonStatusStarted, r.(*testReader).status)

	r = s.readerGroup.NewReader(DefaultReaderId + 1)
	s.Equal(common.DaemonStatusStarted, r.(*testReader).status)

	s.readerGroup.Stop()
	readers := s.readerGroup.Readers()
	s.Len(readers, 2)
	for _, r := range readers {
		s.Equal(common.DaemonStatusStopped, r.(*testReader).status)
	}

	r = s.readerGroup.NewReader(DefaultReaderId + 2)
	s.Equal(common.DaemonStatusInitialized, r.(*testReader).status)
}

func (s *readerGroupSuite) TestAddGetReader() {
	s.Empty(s.readerGroup.Readers())

	r, ok := s.readerGroup.ReaderByID(DefaultReaderId)
	s.False(ok)
	s.Nil(r)

	for i := int32(0); i < 3; i++ {
		r = s.readerGroup.NewReader(i)

		readers := s.readerGroup.Readers()
		s.Len(readers, int(i)+1)
		s.Equal(r, readers[i])

		retrievedReader, ok := s.readerGroup.ReaderByID(i)
		s.True(ok)
		s.Equal(r, retrievedReader)
	}

	s.Panics(func() {
		s.readerGroup.NewReader(DefaultReaderId)
	})
}

func (s *readerGroupSuite) TestRemoveReader() {
	s.readerGroup.Start()
	defer s.readerGroup.Stop()

	r := s.readerGroup.NewReader(DefaultReaderId)
	s.readerGroup.RemoveReader(DefaultReaderId)
	s.Equal(common.DaemonStatusStopped, r.(*testReader).status)
	s.Len(s.readerGroup.Readers(), 0)
}

func newTestReader() Reader {
	return &testReader{
		status: common.DaemonStatusInitialized,
	}
}

func (r *testReader) Start()                       { r.status = common.DaemonStatusStarted }
func (r *testReader) Stop()                        { r.status = common.DaemonStatusStopped }
func (r *testReader) Scopes() []Scope              { panic("not implemented") }
func (r *testReader) WalkSlices(SliceIterator)     { panic("not implemented") }
func (r *testReader) SplitSlices(SliceSplitter)    { panic("not implemented") }
func (r *testReader) MergeSlices(...Slice)         { panic("not implemented") }
func (r *testReader) AppendSlices(...Slice)        { panic("not implemented") }
func (r *testReader) ClearSlices(SlicePredicate)   { panic("not implemented") }
func (r *testReader) CompactSlices(SlicePredicate) { panic("not implemented") }
func (r *testReader) ShrinkSlices()                { panic("not implemented") }
func (r *testReader) Pause(time.Duration)          { panic("not implemented") }
