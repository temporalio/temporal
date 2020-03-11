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
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"

	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/loggerimpl"
)

type (
	cadenceSerializerSuite struct {
		suite.Suite
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
		logger log.Logger
	}
)

func TestCadenceSerializerSuite(t *testing.T) {
	s := new(cadenceSerializerSuite)
	suite.Run(t, s)
}

func (s *cadenceSerializerSuite) SetupTest() {
	var err error
	s.logger, err = loggerimpl.NewDevelopment()
	s.Require().NoError(err)
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
}

func (s *cadenceSerializerSuite) TestSerializer() {

	concurrency := 1
	startWG := sync.WaitGroup{}
	doneWG := sync.WaitGroup{}

	startWG.Add(1)
	doneWG.Add(concurrency)

	serializer := NewPayloadSerializer()

	eventType := enums.EventTypeActivityTaskCompleted
	event0 := &commonproto.HistoryEvent{
		EventId:   999,
		Timestamp: time.Now().UnixNano(),
		EventType: eventType,
		Attributes: &commonproto.HistoryEvent_ActivityTaskCompletedEventAttributes{
			ActivityTaskCompletedEventAttributes: &commonproto.ActivityTaskCompletedEventAttributes{
				Result:           []byte("result-1-event-1"),
				ScheduledEventId: 4,
				StartedEventId:   5,
				Identity:         "event-1",
			},
		},
	}

	history0 := &commonproto.History{Events: []*commonproto.HistoryEvent{event0, event0}}

	memoFields := map[string][]byte{
		"TestField": []byte(`Test binary`),
	}
	memo0 := &commonproto.Memo{Fields: memoFields}

	resetPoints0 := &commonproto.ResetPoints{
		Points: []*commonproto.ResetPointInfo{
			{
				BinaryChecksum:           "bad-binary-cs",
				RunId:                    "test-run-id",
				FirstDecisionCompletedId: 123,
				CreatedTimeNano:          456,
				ExpiringTimeNano:         789,
				Resettable:               true,
			},
		},
	}

	badBinaries0 := &commonproto.BadBinaries{
		Binaries: map[string]*commonproto.BadBinaryInfo{
			"bad-binary-cs": {
				CreatedTimeNano: 456,
				Operator:        "test-operattor",
				Reason:          "test-reason",
			},
		},
	}

	histories := &commonproto.VersionHistories{
		Histories: []*commonproto.VersionHistory{
			{
				BranchToken: []byte{1},
				Items: []*commonproto.VersionHistoryItem{
					{
						EventID: 1,
						Version: 0,
					},
					{
						EventID: 2,
						Version: 1,
					},
				},
			},
			{
				BranchToken: []byte{2},
				Items: []*commonproto.VersionHistoryItem{
					{
						EventID: 2,
						Version: 0,
					},
					{
						EventID: 3,
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

			nilEvent, err := serializer.SerializeEvent(nil, common.EncodingTypeProto3)
			s.Nil(err)
			s.Nil(nilEvent)

			_, err = serializer.SerializeEvent(event0, common.EncodingTypeGob)
			s.NotNil(err)
			_, ok := err.(*UnknownEncodingTypeError)
			s.True(ok)

			dJSON, err := serializer.SerializeEvent(event0, common.EncodingTypeJSON)
			s.Nil(err)
			s.NotNil(dJSON)

			dThrift, err := serializer.SerializeEvent(event0, common.EncodingTypeProto3)
			s.Nil(err)
			s.NotNil(dThrift)

			dEmpty, err := serializer.SerializeEvent(event0, common.EncodingType(""))
			s.Nil(err)
			s.NotNil(dEmpty)

			// serialize batch events

			nilEvents, err := serializer.SerializeBatchEvents(nil, common.EncodingTypeProto3)
			s.Nil(err)
			s.NotNil(nilEvents)

			_, err = serializer.SerializeBatchEvents(history0.Events, common.EncodingTypeGob)
			s.NotNil(err)
			_, ok = err.(*UnknownEncodingTypeError)
			s.True(ok)

			dsJSON, err := serializer.SerializeBatchEvents(history0.Events, common.EncodingTypeJSON)
			s.Nil(err)
			s.NotNil(dsJSON)

			dsProto, err := serializer.SerializeBatchEvents(history0.Events, common.EncodingTypeProto3)
			s.Nil(err)
			s.NotNil(dsProto)

			dsEmpty, err := serializer.SerializeBatchEvents(history0.Events, common.EncodingType(""))
			s.Nil(err)
			s.NotNil(dsEmpty)

			_, err = serializer.SerializeVisibilityMemo(memo0, common.EncodingTypeGob)
			s.NotNil(err)
			_, ok = err.(*UnknownEncodingTypeError)
			s.True(ok)

			// serialize visibility memo

			nilMemo, err := serializer.SerializeVisibilityMemo(nil, common.EncodingTypeProto3)
			s.Nil(err)
			s.Nil(nilMemo)

			mJSON, err := serializer.SerializeVisibilityMemo(memo0, common.EncodingTypeJSON)
			s.Nil(err)
			s.NotNil(mJSON)

			mThrift, err := serializer.SerializeVisibilityMemo(memo0, common.EncodingTypeProto3)
			s.Nil(err)
			s.NotNil(mThrift)

			mEmpty, err := serializer.SerializeVisibilityMemo(memo0, common.EncodingType(""))
			s.Nil(err)
			s.NotNil(mEmpty)

			// serialize version histories

			nilHistories, err := serializer.SerializeVersionHistories(nil, common.EncodingTypeThriftRW)
			s.Nil(err)
			s.Nil(nilHistories)

			historiesJSON, err := serializer.SerializeVersionHistories(histories, common.EncodingTypeJSON)
			s.Nil(err)
			s.NotNil(historiesJSON)

			historiesThrift, err := serializer.SerializeVersionHistories(histories, common.EncodingTypeThriftRW)
			s.Nil(err)
			s.NotNil(historiesThrift)

			historiesEmpty, err := serializer.SerializeVersionHistories(histories, common.EncodingType(""))
			s.Nil(err)
			s.NotNil(historiesEmpty)

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

			event3, err := serializer.DeserializeEvent(dEmpty)
			s.Nil(err)
			s.True(reflect.DeepEqual(event0, event3))

			// deserialize batch events

			dNilEvents, err := serializer.DeserializeBatchEvents(nilEvents)
			s.Nil(err)
			s.Nil(dNilEvents)

			events, err := serializer.DeserializeBatchEvents(dsJSON)
			history1 := &commonproto.History{Events: events}
			s.Nil(err)
			s.True(reflect.DeepEqual(history0, history1))

			events, err = serializer.DeserializeBatchEvents(dsProto)
			history2 := &commonproto.History{Events: events}
			s.Nil(err)
			s.True(reflect.DeepEqual(history0, history2))

			events, err = serializer.DeserializeBatchEvents(dsEmpty)
			history3 := &commonproto.History{Events: events}
			s.Nil(err)
			s.True(reflect.DeepEqual(history0, history3))

			// deserialize visibility memo

			dNilMemo, err := serializer.DeserializeVisibilityMemo(nilMemo)
			s.Nil(err)
			s.Equal(&commonproto.Memo{}, dNilMemo)

			memo1, err := serializer.DeserializeVisibilityMemo(mJSON)
			s.Nil(err)
			s.True(reflect.DeepEqual(memo0, memo1))

			memo2, err := serializer.DeserializeVisibilityMemo(mThrift)
			s.Nil(err)
			s.True(reflect.DeepEqual(memo0, memo2))
			memo3, err := serializer.DeserializeVisibilityMemo(mEmpty)
			s.Nil(err)
			s.True(reflect.DeepEqual(memo0, memo3))

			// serialize reset points

			nilResetPoints, err := serializer.SerializeResetPoints(nil, common.EncodingTypeThriftRW)
			s.Nil(err)
			s.NotNil(nilResetPoints)

			_, err = serializer.SerializeResetPoints(resetPoints0, common.EncodingTypeGob)
			s.NotNil(err)
			_, ok = err.(*UnknownEncodingTypeError)
			s.True(ok)

			resetPointsJSON, err := serializer.SerializeResetPoints(resetPoints0, common.EncodingTypeJSON)
			s.Nil(err)
			s.NotNil(resetPointsJSON)

			resetPointsThrift, err := serializer.SerializeResetPoints(resetPoints0, common.EncodingTypeThriftRW)
			s.Nil(err)
			s.NotNil(resetPointsThrift)

			resetPointsEmpty, err := serializer.SerializeResetPoints(resetPoints0, common.EncodingType(""))
			s.Nil(err)
			s.NotNil(resetPointsEmpty)

			// deserialize reset points

			dNilResetPoints1, err := serializer.DeserializeResetPoints(nil)
			s.Nil(err)
			s.Equal(&commonproto.ResetPoints{}, dNilResetPoints1)

			dNilResetPoints2, err := serializer.DeserializeResetPoints(nilResetPoints)
			s.Nil(err)
			s.Equal(&commonproto.ResetPoints{}, dNilResetPoints2)

			resetPoints1, err := serializer.DeserializeResetPoints(resetPointsJSON)
			s.Nil(err)
			s.True(reflect.DeepEqual(resetPoints1, resetPoints0))

			resetPoints2, err := serializer.DeserializeResetPoints(resetPointsThrift)
			s.Nil(err)
			s.True(reflect.DeepEqual(resetPoints2, resetPoints0))

			resetPoints3, err := serializer.DeserializeResetPoints(resetPointsEmpty)
			s.Nil(err)
			s.True(reflect.DeepEqual(resetPoints3, resetPoints0))

			// serialize bad binaries

			nilBadBinaries, err := serializer.SerializeBadBinaries(nil, common.EncodingTypeThriftRW)
			s.Nil(err)
			s.NotNil(nilBadBinaries)

			_, err = serializer.SerializeBadBinaries(badBinaries0, common.EncodingTypeGob)
			s.NotNil(err)
			_, ok = err.(*UnknownEncodingTypeError)
			s.True(ok)

			badBinariesJSON, err := serializer.SerializeBadBinaries(badBinaries0, common.EncodingTypeJSON)
			s.Nil(err)
			s.NotNil(badBinariesJSON)

			badBinariesThrift, err := serializer.SerializeBadBinaries(badBinaries0, common.EncodingTypeThriftRW)
			s.Nil(err)
			s.NotNil(badBinariesThrift)

			badBinariesEmpty, err := serializer.SerializeBadBinaries(badBinaries0, common.EncodingType(""))
			s.Nil(err)
			s.NotNil(badBinariesEmpty)

			// deserialize bad binaries

			dNilBadBinaries1, err := serializer.DeserializeBadBinaries(nil)
			s.Nil(err)
			s.Equal(&commonproto.BadBinaries{}, dNilBadBinaries1)

			dNilBadBinaries2, err := serializer.DeserializeBadBinaries(nilBadBinaries)
			s.Nil(err)
			s.Equal(&commonproto.BadBinaries{}, dNilBadBinaries2)

			badBinaries1, err := serializer.DeserializeBadBinaries(badBinariesJSON)
			s.Nil(err)
			s.True(reflect.DeepEqual(badBinaries1, badBinaries0))

			badBinaries2, err := serializer.DeserializeBadBinaries(badBinariesThrift)
			s.Nil(err)
			s.True(reflect.DeepEqual(badBinaries2, badBinaries0))

			badBinaries3, err := serializer.DeserializeBadBinaries(badBinariesEmpty)
			s.Nil(err)
			s.True(reflect.DeepEqual(badBinaries3, badBinaries0))

			// serialize version histories

			dNilHistories, err := serializer.DeserializeVersionHistories(nil)
			s.Nil(err)
			s.Equal(&commonproto.VersionHistories{}, dNilHistories)

			dNilHistories2, err := serializer.DeserializeVersionHistories(nilHistories)
			s.Nil(err)
			s.Equal(&commonproto.VersionHistories{}, dNilHistories2)

			dHistoriesJSON, err := serializer.DeserializeVersionHistories(historiesJSON)
			s.Nil(err)
			s.True(reflect.DeepEqual(dHistoriesJSON, histories))

			dHistoriesThrift, err := serializer.DeserializeVersionHistories(historiesThrift)
			s.Nil(err)
			s.True(reflect.DeepEqual(dHistoriesThrift, histories))

			dHistoriesEmpty, err := serializer.DeserializeVersionHistories(historiesEmpty)
			s.Nil(err)
			s.True(reflect.DeepEqual(dHistoriesEmpty, histories))
		}()
	}

	startWG.Done()
	succ := common.AwaitWaitGroup(&doneWG, 10*time.Second)
	s.True(succ, "test timed out")
}
