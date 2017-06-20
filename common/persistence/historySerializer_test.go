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

package persistence

import (
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"sync"
	"testing"
	"time"
)

type (
	historySerializerSuite struct {
		suite.Suite
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
		logger bark.Logger
	}
)

func TestHistoryBuilderSuite(t *testing.T) {
	s := new(historySerializerSuite)
	suite.Run(t, s)
}

func (s *historySerializerSuite) SetupTest() {
	s.logger = bark.NewLoggerFromLogrus(log.New())
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
}

func (s *historySerializerSuite) TestSerializerFactory() {

	concurrency := 5
	startWG := sync.WaitGroup{}
	doneWG := sync.WaitGroup{}

	startWG.Add(1)
	doneWG.Add(concurrency)

	factory := NewHistorySerializerFactory()

	event1 := &workflow.HistoryEvent{
		EventId:   common.Int64Ptr(999),
		Timestamp: common.Int64Ptr(time.Now().UnixNano()),
		EventType: common.EventTypePtr(workflow.EventType_ActivityTaskCompleted),
		ActivityTaskCompletedEventAttributes: &workflow.ActivityTaskCompletedEventAttributes{
			Result_:          []byte("result-1-event-1"),
			ScheduledEventId: common.Int64Ptr(4),
			StartedEventId:   common.Int64Ptr(5),
			Identity:         common.StringPtr("event-1"),
		},
	}

	for i := 0; i < concurrency; i++ {

		go func() {

			startWG.Wait()
			defer doneWG.Done()

			serializer, err := factory.Get(common.EncodingTypeGob)
			s.NotNil(err)
			_, ok := err.(*UnknownEncodingTypeError)
			s.True(ok)

			serializer, err = factory.Get(common.EncodingTypeJSON)
			s.Nil(err)
			s.NotNil(serializer)
			_, ok = serializer.(*jsonHistorySerializer)
			s.True(ok)

			events := []*workflow.HistoryEvent{event1}
			eventBatch := NewHistoryEventBatch(GetMaxSupportedHistoryVersion()+1, events)
			sh, err := serializer.Serialize(eventBatch)
			s.NotNil(err)
			_, ok = err.(*HistorySerializationError)
			s.True(ok)

			eventBatch.Version = 1
			sh, err = serializer.Serialize(eventBatch)
			s.Nil(err)
			s.NotNil(sh)
			s.Equal(1, sh.Version)
			s.Equal(common.EncodingTypeJSON, sh.EncodingType)

			sh.Version = 2
			dh, err := serializer.Deserialize(sh)
			s.NotNil(err)
			_, ok = err.(*HistoryDeserializationError)
			s.True(ok)
			s.Nil(dh)

			sh.Version = 1
			dh, err = serializer.Deserialize(sh)
			s.Nil(err)
			s.NotNil(dh)

			s.Equal(dh.Version, 1)
			s.Equal(len(dh.Events), 1)
			s.Equal(event1.GetEventId(), dh.Events[0].GetEventId())
			s.Equal(event1.GetTimestamp(), dh.Events[0].GetTimestamp())
			s.Equal(event1.GetEventType(), dh.Events[0].GetEventType())
			s.Equal(event1.GetActivityTaskCompletedEventAttributes().GetResult_(), dh.Events[0].GetActivityTaskCompletedEventAttributes().GetResult_())

		}()
	}

	startWG.Done()
	succ := common.AwaitWaitGroup(&doneWG, 10*time.Second)
	s.True(succ, "test timed out")
}
