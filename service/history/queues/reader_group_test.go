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
	"math/rand"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
)

type (
	readerGroupSuite struct {
		suite.Suite
		*require.Assertions

		controller           *gomock.Controller
		mockExecutionManager *persistence.MockExecutionManager

		shardID  int32
		category tasks.Category

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

	s.controller = gomock.NewController(s.T())
	s.mockExecutionManager = persistence.NewMockExecutionManager(s.controller)

	s.shardID = rand.Int31()
	s.category = tasks.CategoryTransfer

	s.readerGroup = NewReaderGroup(
		s.shardID,
		s.category,
		func(_ int32, _ []Slice) Reader {
			return newTestReader()
		},
		s.mockExecutionManager,
	)
}

func (s *readerGroupSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *readerGroupSuite) TestStartStop() {
	readerID := int32(DefaultReaderId)
	s.setupRegisterReaderMock(readerID)
	r := s.readerGroup.NewReader(readerID)
	s.Equal(common.DaemonStatusInitialized, r.(*testReader).status)

	s.readerGroup.Start()
	s.Equal(common.DaemonStatusStarted, r.(*testReader).status)

	readerID = int32(DefaultReaderId + 1)
	s.setupRegisterReaderMock(readerID)
	r = s.readerGroup.NewReader(readerID)
	s.Equal(common.DaemonStatusStarted, r.(*testReader).status)

	var readers []*testReader
	for readerID, reader := range s.readerGroup.Readers() {
		s.setupUnRegisterReaderMock(readerID)
		readers = append(readers, reader.(*testReader))
	}
	s.readerGroup.Stop()
	s.Empty(s.readerGroup.Readers(), 2)
	for _, r := range readers {
		s.Equal(common.DaemonStatusStopped, r.status)
	}

	readerID = int32(DefaultReaderId + 2)
	s.setupRegisterReaderMock(readerID)
	r = s.readerGroup.NewReader(readerID)
	s.Equal(common.DaemonStatusInitialized, r.(*testReader).status)
}

func (s *readerGroupSuite) TestAddGetReader() {
	s.Empty(s.readerGroup.Readers())

	r, ok := s.readerGroup.ReaderByID(DefaultReaderId)
	s.False(ok)
	s.Nil(r)

	for i := int32(0); i < 3; i++ {
		s.setupRegisterReaderMock(i)
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

	readerID := int32(DefaultReaderId)

	s.setupRegisterReaderMock(readerID)
	r := s.readerGroup.NewReader(readerID)

	s.setupUnRegisterReaderMock(readerID)
	s.readerGroup.RemoveReader(readerID)

	s.Equal(common.DaemonStatusStopped, r.(*testReader).status)
	s.Len(s.readerGroup.Readers(), 0)
}

func (s *readerGroupSuite) TestForEach() {
	// TODO
}

func (s *readerGroupSuite) setupRegisterReaderMock(
	readerID int32,
) {
	request := &persistence.RegisterHistoryTaskReaderRequest{
		ShardID:      s.shardID,
		TaskCategory: s.category,
		ReaderID:     readerID,
	}

	s.mockExecutionManager.EXPECT().RegisterHistoryTaskReader(gomock.Any(), request).Return(nil).Times(1)
}

func (s *readerGroupSuite) setupUnRegisterReaderMock(
	readerID int32,
) {
	request := &persistence.UnregisterHistoryTaskReaderRequest{
		ShardID:      s.shardID,
		TaskCategory: s.category,
		ReaderID:     readerID,
	}

	s.mockExecutionManager.EXPECT().UnregisterHistoryTaskReader(gomock.Any(), request).Times(1)
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
func (r *testReader) Notify()                      { panic("not implemented") }
func (r *testReader) Pause(time.Duration)          { panic("not implemented") }
