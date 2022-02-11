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

package serialization

import (
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives/timestamp"
)

type (
	temporalSerializerSuite struct {
		suite.Suite
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
		logger log.Logger
	}
)

func TestTemporalSerializerSuite(t *testing.T) {
	s := new(temporalSerializerSuite)
	suite.Run(t, s)
}

func (s *temporalSerializerSuite) SetupTest() {
	s.logger = log.NewTestLogger()
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
}

func (s *temporalSerializerSuite) TestSerializer() {

	concurrency := 1
	startWG := sync.WaitGroup{}
	doneWG := sync.WaitGroup{}

	startWG.Add(1)
	doneWG.Add(concurrency)

	serializer := NewSerializer()

	eventType := enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED
	event0 := &historypb.HistoryEvent{
		EventId:   999,
		EventTime: timestamp.TimePtr(time.Date(2020, 8, 22, 0, 0, 0, 0, time.UTC)),
		EventType: eventType,
		Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{
			ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
				Result:           payloads.EncodeString("result-1-event-1"),
				ScheduledEventId: 4,
				StartedEventId:   5,
				Identity:         "event-1",
			},
		},
	}

	history0 := &historypb.History{Events: []*historypb.HistoryEvent{event0, event0}}

	for i := 0; i < concurrency; i++ {

		go func() {

			startWG.Wait()
			defer doneWG.Done()

			// serialize event

			nilEvent, err := serializer.SerializeEvent(nil, enumspb.ENCODING_TYPE_PROTO3)
			s.Nil(err)
			s.Nil(nilEvent)

			_, err = serializer.SerializeEvent(event0, enumspb.ENCODING_TYPE_UNSPECIFIED)
			s.NotNil(err)
			_, ok := err.(*UnknownEncodingTypeError)
			s.True(ok)

			dProto, err := serializer.SerializeEvent(event0, enumspb.ENCODING_TYPE_PROTO3)
			s.Nil(err)
			s.NotNil(dProto)

			// serialize batch events

			nilEvents, err := serializer.SerializeEvents(nil, enumspb.ENCODING_TYPE_PROTO3)
			s.Nil(err)
			s.NotNil(nilEvents)

			_, err = serializer.SerializeEvents(history0.Events, enumspb.ENCODING_TYPE_UNSPECIFIED)
			s.NotNil(err)
			_, ok = err.(*UnknownEncodingTypeError)
			s.True(ok)

			dsProto, err := serializer.SerializeEvents(history0.Events, enumspb.ENCODING_TYPE_PROTO3)
			s.Nil(err)
			s.NotNil(dsProto)

			// deserialize event

			dNilEvent, err := serializer.DeserializeEvent(nilEvent)
			s.Nil(err)
			s.Nil(dNilEvent)

			event2, err := serializer.DeserializeEvent(dProto)
			s.Nil(err)
			s.True(reflect.DeepEqual(event0, event2))

			// deserialize events

			dNilEvents, err := serializer.DeserializeEvents(nilEvents)
			s.Nil(err)
			s.Nil(dNilEvents)

			events, err := serializer.DeserializeEvents(dsProto)
			history2 := &historypb.History{Events: events}
			s.Nil(err)
			s.True(reflect.DeepEqual(history0, history2))
		}()
	}

	startWG.Done()
	succ := common.AwaitWaitGroup(&doneWG, 10*time.Second)
	s.True(succ, "test timed out")
}
