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

package history

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/cadence/testsuite"
)

type workflowWatcherSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
}

func TestWorkflowWatcherSuite(t *testing.T) {
	suite.Run(t, new(workflowWatcherSuite))
}

func (s *workflowWatcherSuite) TestWorkflowWatcher_NoSubscribers() {
	ps := NewWorkflowWatcher()
	state := s.newRandomWatcherUpdate()
	ps.Publish(state)
	s.assertWatcherUpdateEqual(state, ps.GetLatestSnapshot())
}

func (s *workflowWatcherSuite) TestWorkflowWatcher_NilPublishAndAccess() {
	ps := NewWorkflowWatcher()
	s.Nil(ps.GetLatestSnapshot())
	ps.Publish(nil)
	s.Nil(ps.GetLatestSnapshot())
}

func (s *workflowWatcherSuite) TestWorkflowWatcher_ZombieUnsubscribe() {
	ps := NewWorkflowWatcher()
	ps.Unsubscribe(-1)
}

func (s *workflowWatcherSuite) TestWorkflowWatcher_ManySubscribers() {
	ps := NewWorkflowWatcher()
	var ids []int64
	var chans []<-chan struct{}
	for i := 0; i < 10; i++ {
		id, ch := ps.Subscribe()
		ids = append(ids, id)
		chans = append(chans, ch)
	}
	for i := 0; i < 3; i++ {
		index := rand.Intn(len(ids))
		ids = append(ids[:index], ids[index+1:]...)
		chans = append(chans[:index], chans[index+1:]...)
	}
	s.assertChanLengthsEqual(0, chans)
	state1 := s.newRandomWatcherUpdate()
	ps.Publish(state1)
	s.assertChanLengthsEqual(1, chans)
	s.assertWatcherUpdateEqual(state1, ps.GetLatestSnapshot())
	state2 := s.newRandomWatcherUpdate()
	ps.Publish(state2)
	s.assertChanLengthsEqual(1, chans)
	s.assertWatcherUpdateEqual(state2, ps.GetLatestSnapshot())
	for _, ch := range chans {
		<-ch
	}
	s.assertChanLengthsEqual(0, chans)
	s.assertWatcherUpdateEqual(state2, ps.GetLatestSnapshot())

}

func (s *workflowWatcherSuite) newRandomWatcherUpdate() *WatcherSnapshot {
	return &WatcherSnapshot{
		CloseStatus: rand.Intn(100),
	}
}

func (s *workflowWatcherSuite) assertWatcherUpdateEqual(expected, actual *WatcherSnapshot) {
	s.Equal(expected.CloseStatus, actual.CloseStatus)
}

func (s *workflowWatcherSuite) assertChanLengthsEqual(expectedLengths int, chans []<-chan struct{}) {
	for _, ch := range chans {
		s.Equal(expectedLengths, len(ch))
	}
}
