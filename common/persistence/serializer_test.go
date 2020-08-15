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

package persistence

import (
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	workflowpb "go.temporal.io/api/workflow/v1"

	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/loggerimpl"
	"go.temporal.io/server/common/payload"
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
	var err error
	s.logger, err = loggerimpl.NewDevelopment()
	s.Require().NoError(err)
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
}

func (s *temporalSerializerSuite) TestSerializer() {

	concurrency := 1
	startWG := sync.WaitGroup{}
	doneWG := sync.WaitGroup{}

	startWG.Add(1)
	doneWG.Add(concurrency)

	serializer := NewPayloadSerializer()

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

	memoFields := map[string]*commonpb.Payload{
		"TestField": payload.EncodeString("Test binary"),
	}
	memo0 := &commonpb.Memo{Fields: memoFields}

	resetPoints0 := &workflowpb.ResetPoints{
		Points: []*workflowpb.ResetPointInfo{
			{
				BinaryChecksum:               "bad-binary-cs",
				RunId:                        "test-run-id",
				FirstWorkflowTaskCompletedId: 123,
				CreateTime:                   timestamp.TimePtr(time.Date(2020, 8, 22, 0, 0, 0, 0, time.UTC)),
				Resettable:                   true,
				ExpireTime:                   timestamp.TimePtr(time.Date(2020, 8, 22, 0, 0, 0, 0, time.UTC)),
			},
		},
	}

	badBinaries0 := &namespacepb.BadBinaries{
		Binaries: map[string]*namespacepb.BadBinaryInfo{
			"bad-binary-cs": {
				CreateTime: timestamp.TimePtr(time.Date(2020, 8, 22, 0, 0, 0, 0, time.UTC)),
				Operator:   "test-operattor",
				Reason:     "test-reason",
			},
		},
	}

	histories := &historyspb.VersionHistories{
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte{1},
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: 1,
						Version: 0,
					},
					{
						EventId: 2,
						Version: 1,
					},
				},
			},
			{
				BranchToken: []byte{2},
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: 2,
						Version: 0,
					},
					{
						EventId: 3,
						Version: 1,
					},
				},
			},
		},
	}

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

			dJSON, err := serializer.SerializeEvent(event0, enumspb.ENCODING_TYPE_JSON)
			s.Nil(err)
			s.NotNil(dJSON)

			dThrift, err := serializer.SerializeEvent(event0, enumspb.ENCODING_TYPE_PROTO3)
			s.Nil(err)
			s.NotNil(dThrift)

			// serialize batch events

			nilEvents, err := serializer.SerializeBatchEvents(nil, enumspb.ENCODING_TYPE_PROTO3)
			s.Nil(err)
			s.NotNil(nilEvents)

			_, err = serializer.SerializeBatchEvents(history0.Events, enumspb.ENCODING_TYPE_UNSPECIFIED)
			s.NotNil(err)
			_, ok = err.(*UnknownEncodingTypeError)
			s.True(ok)

			dsJSON, err := serializer.SerializeBatchEvents(history0.Events, enumspb.ENCODING_TYPE_JSON)
			s.Nil(err)
			s.NotNil(dsJSON)

			dsProto, err := serializer.SerializeBatchEvents(history0.Events, enumspb.ENCODING_TYPE_PROTO3)
			s.Nil(err)
			s.NotNil(dsProto)

			_, err = serializer.SerializeVisibilityMemo(memo0, enumspb.ENCODING_TYPE_UNSPECIFIED)
			s.NotNil(err)
			_, ok = err.(*UnknownEncodingTypeError)
			s.True(ok)

			// serialize visibility memo

			nilMemo, err := serializer.SerializeVisibilityMemo(nil, enumspb.ENCODING_TYPE_PROTO3)
			s.Nil(err)
			s.Nil(nilMemo)

			mJSON, err := serializer.SerializeVisibilityMemo(memo0, enumspb.ENCODING_TYPE_JSON)
			s.Nil(err)
			s.NotNil(mJSON)

			mThrift, err := serializer.SerializeVisibilityMemo(memo0, enumspb.ENCODING_TYPE_PROTO3)
			s.Nil(err)
			s.NotNil(mThrift)

			// serialize version histories

			nilHistories, err := serializer.SerializeVersionHistories(nil, enumspb.ENCODING_TYPE_PROTO3)
			s.Nil(err)
			s.Nil(nilHistories)

			historiesJSON, err := serializer.SerializeVersionHistories(histories, enumspb.ENCODING_TYPE_JSON)
			s.Nil(err)
			s.NotNil(historiesJSON)

			historiesThrift, err := serializer.SerializeVersionHistories(histories, enumspb.ENCODING_TYPE_PROTO3)
			s.Nil(err)
			s.NotNil(historiesThrift)

			// deserialize event

			dNilEvent, err := serializer.DeserializeEvent(nilEvent)
			s.Nil(err)
			s.Nil(dNilEvent)

			event1, err := serializer.DeserializeEvent(dJSON)
			s.Nil(err)
			s.True(reflect.DeepEqual(event0, event1))

			event2, err := serializer.DeserializeEvent(dThrift)
			s.Nil(err)
			s.True(reflect.DeepEqual(event0, event2))

			// deserialize batch events

			dNilEvents, err := serializer.DeserializeBatchEvents(nilEvents)
			s.Nil(err)
			s.Nil(dNilEvents)

			events, err := serializer.DeserializeBatchEvents(dsJSON)
			history1 := &historypb.History{Events: events}
			s.Nil(err)
			s.True(reflect.DeepEqual(history0, history1))

			events, err = serializer.DeserializeBatchEvents(dsProto)
			history2 := &historypb.History{Events: events}
			s.Nil(err)
			s.True(reflect.DeepEqual(history0, history2))

			// deserialize visibility memo

			dNilMemo, err := serializer.DeserializeVisibilityMemo(nilMemo)
			s.Nil(err)
			s.Equal(&commonpb.Memo{}, dNilMemo)

			memo1, err := serializer.DeserializeVisibilityMemo(mJSON)
			s.Nil(err)
			s.True(reflect.DeepEqual(memo0, memo1))

			memo2, err := serializer.DeserializeVisibilityMemo(mThrift)
			s.Nil(err)
			s.True(reflect.DeepEqual(memo0, memo2))

			// serialize reset points

			nilResetPoints, err := serializer.SerializeResetPoints(nil, enumspb.ENCODING_TYPE_PROTO3)
			s.Nil(err)
			s.NotNil(nilResetPoints)

			_, err = serializer.SerializeResetPoints(resetPoints0, enumspb.ENCODING_TYPE_UNSPECIFIED)
			s.NotNil(err)
			_, ok = err.(*UnknownEncodingTypeError)
			s.True(ok)

			resetPointsJSON, err := serializer.SerializeResetPoints(resetPoints0, enumspb.ENCODING_TYPE_JSON)
			s.Nil(err)
			s.NotNil(resetPointsJSON)

			resetPointsThrift, err := serializer.SerializeResetPoints(resetPoints0, enumspb.ENCODING_TYPE_PROTO3)
			s.Nil(err)
			s.NotNil(resetPointsThrift)

			// deserialize reset points

			dNilResetPoints1, err := serializer.DeserializeResetPoints(nil)
			s.Nil(err)
			s.Equal(&workflowpb.ResetPoints{}, dNilResetPoints1)

			dNilResetPoints2, err := serializer.DeserializeResetPoints(nilResetPoints)
			s.Nil(err)
			s.Equal(&workflowpb.ResetPoints{}, dNilResetPoints2)

			resetPoints1, err := serializer.DeserializeResetPoints(resetPointsJSON)
			s.Nil(err)
			s.True(reflect.DeepEqual(resetPoints1, resetPoints0))

			resetPoints2, err := serializer.DeserializeResetPoints(resetPointsThrift)
			s.Nil(err)
			s.True(reflect.DeepEqual(resetPoints2, resetPoints0))

			// serialize bad binaries

			nilBadBinaries, err := serializer.SerializeBadBinaries(nil, enumspb.ENCODING_TYPE_PROTO3)
			s.Nil(err)
			s.NotNil(nilBadBinaries)

			_, err = serializer.SerializeBadBinaries(badBinaries0, enumspb.ENCODING_TYPE_UNSPECIFIED)
			s.NotNil(err)
			_, ok = err.(*UnknownEncodingTypeError)
			s.True(ok)

			badBinariesJSON, err := serializer.SerializeBadBinaries(badBinaries0, enumspb.ENCODING_TYPE_JSON)
			s.Nil(err)
			s.NotNil(badBinariesJSON)

			badBinariesThrift, err := serializer.SerializeBadBinaries(badBinaries0, enumspb.ENCODING_TYPE_PROTO3)
			s.Nil(err)
			s.NotNil(badBinariesThrift)

			// deserialize bad binaries

			dNilBadBinaries1, err := serializer.DeserializeBadBinaries(nil)
			s.Nil(err)
			s.Equal(&namespacepb.BadBinaries{}, dNilBadBinaries1)

			dNilBadBinaries2, err := serializer.DeserializeBadBinaries(nilBadBinaries)
			s.Nil(err)
			s.Equal(&namespacepb.BadBinaries{}, dNilBadBinaries2)

			badBinaries1, err := serializer.DeserializeBadBinaries(badBinariesJSON)
			s.Nil(err)
			s.True(reflect.DeepEqual(badBinaries1, badBinaries0))

			badBinaries2, err := serializer.DeserializeBadBinaries(badBinariesThrift)
			s.Nil(err)
			s.True(reflect.DeepEqual(badBinaries2, badBinaries0))

			// serialize version histories

			dNilHistories, err := serializer.DeserializeVersionHistories(nil)
			s.Nil(err)
			s.Equal(&historyspb.VersionHistories{}, dNilHistories)

			dNilHistories2, err := serializer.DeserializeVersionHistories(nilHistories)
			s.Nil(err)
			s.Equal(&historyspb.VersionHistories{}, dNilHistories2)

			dHistoriesJSON, err := serializer.DeserializeVersionHistories(historiesJSON)
			s.Nil(err)
			s.True(reflect.DeepEqual(dHistoriesJSON, histories))

			dHistoriesThrift, err := serializer.DeserializeVersionHistories(historiesThrift)
			s.Nil(err)
			s.True(reflect.DeepEqual(dHistoriesThrift, histories))
		}()
	}

	startWG.Done()
	succ := common.AwaitWaitGroup(&doneWG, 10*time.Second)
	s.True(succ, "test timed out")
}
