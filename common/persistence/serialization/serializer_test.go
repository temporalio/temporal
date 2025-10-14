package serialization

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/fakedata"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	temporalSerializerSuite struct {
		suite.Suite
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
		protorequire.ProtoAssertions
		logger log.Logger

		serializer Serializer
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
	s.ProtoAssertions = protorequire.New(s.T())

	s.serializer = NewSerializer()
}

func (s *temporalSerializerSuite) TestSerializer() {
	concurrency := 1
	startWG := sync.WaitGroup{}
	doneWG := sync.WaitGroup{}

	startWG.Add(1)
	doneWG.Add(concurrency)

	eventType := enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED
	event0 := &historypb.HistoryEvent{
		EventId:   999,
		EventTime: timestamppb.New(time.Date(2020, 8, 22, 0, 0, 0, 0, time.UTC)),
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
			nilEvent, err := s.serializer.SerializeEvent(nil)
			s.Nil(err)
			s.Nil(nilEvent)

			dProto, err := s.serializer.SerializeEvent(event0)
			s.Nil(err)
			s.NotNil(dProto)

			// serialize batch events
			nilEvents, err := s.serializer.SerializeEvents(nil)
			s.Nil(err)
			s.NotNil(nilEvents)

			dsProto, err := s.serializer.SerializeEvents(history0.Events)
			s.Nil(err)
			s.NotNil(dsProto)

			// deserialize event
			dNilEvent, err := s.serializer.DeserializeEvent(nilEvent)
			s.Nil(err)
			s.Nil(dNilEvent)

			event2, err := s.serializer.DeserializeEvent(dProto)
			s.Nil(err)
			s.ProtoEqual(event0, event2)

			// deserialize events
			dNilEvents, err := s.serializer.DeserializeEvents(nilEvents)
			s.Nil(err)
			s.Nil(dNilEvents)

			events, err := s.serializer.DeserializeEvents(dsProto)
			history2 := &historypb.History{Events: events}
			s.Nil(err)
			s.ProtoEqual(history0, history2)
		}()
	}

	startWG.Done()
	succ := common.AwaitWaitGroup(&doneWG, 10*time.Second)
	s.True(succ, "test timed out")
}

func (s *temporalSerializerSuite) TestSerializeShardInfo_EmptyMapSlice() {
	var shardInfo persistencespb.ShardInfo

	shardInfo.ShardId = rand.Int31()
	shardInfo.RangeId = rand.Int63()

	categoryID := rand.Int31()
	shardInfo.QueueStates = make(map[int32]*persistencespb.QueueState)
	shardInfo.QueueStates[categoryID] = &persistencespb.QueueState{
		ReaderStates: make(map[int64]*persistencespb.QueueReaderState),
	}
	shardInfo.QueueStates[categoryID].ReaderStates[rand.Int63()] = &persistencespb.QueueReaderState{
		Scopes: make([]*persistencespb.QueueSliceScope, 0),
	}
	shardInfo.ReplicationDlqAckLevel = make(map[string]int64)

	blob, err := s.serializer.ShardInfoToBlob(&shardInfo)
	s.NoError(err)

	deserializedShardInfo, err := s.serializer.ShardInfoFromBlob(blob)
	s.NoError(err)
	s.NotNil(deserializedShardInfo)

	s.ProtoEqual(&shardInfo, deserializedShardInfo)
}

func (s *temporalSerializerSuite) TestSerializeShardInfo_Random() {
	var shardInfo persistencespb.ShardInfo
	err := fakedata.FakeStruct(&shardInfo)
	s.NoError(err)

	blob, err := s.serializer.ShardInfoToBlob(&shardInfo)
	s.NoError(err)

	deserializedShardInfo, err := s.serializer.ShardInfoFromBlob(blob)
	s.NoError(err)
	s.NotNil(deserializedShardInfo)

	s.ProtoEqual(&shardInfo, deserializedShardInfo)
}

func (s *temporalSerializerSuite) TestDeserializeStrippedEvents() {
	// 1. Nil data blob
	s.Run("NilDataBlob", func() {
		events, err := s.serializer.DeserializeStrippedEvents(nil)
		s.NoError(err)
		s.Nil(events)
	})

	// 2. Empty data
	s.Run("EmptyDataBlob", func() {
		// Data is nil
		events, err := s.serializer.DeserializeStrippedEvents(&commonpb.DataBlob{
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			Data:         nil,
		})
		s.NoError(err)
		s.Nil(events)

		// Data is empty byte array
		events, err = s.serializer.DeserializeStrippedEvents(&commonpb.DataBlob{
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			Data:         []byte{},
		})
		s.NoError(err)
		s.Nil(events)
	})

	// 3. Unknown encoding type
	s.Run("UnknownEncodingType", func() {
		_, err := s.serializer.DeserializeStrippedEvents(&commonpb.DataBlob{
			EncodingType: enumspb.ENCODING_TYPE_JSON, // Not handled by our switch
			Data:         []byte("irrelevant-data"),
		})
		s.Error(err)
		s.Contains(err.Error(), "unknown or unsupported encoding type")
	})

	// 4. Proper proto decoding, discarding unknown fields
	s.Run("ProtoDiscardUnknownFields", func() {
		// Build a HistoryEvent that contains fields *not* present in StrippedHistoryEvent
		historyEvent := &historypb.HistoryEvent{
			EventId:   123,
			Version:   456,
			EventTime: nil, // or a valid timestamp
			// This is an extra field not present in StrippedHistoryEvent
			Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
				WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
					WorkflowType: &commonpb.WorkflowType{Name: "some-workflow-type"},
				},
			},
		}

		historyEvents := &historypb.History{
			Events: []*historypb.HistoryEvent{historyEvent},
		}

		// Marshal to protobuf
		data, err := historyEvents.Marshal()
		s.Require().NoError(err)

		dataBlob := &commonpb.DataBlob{
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			Data:         data,
		}

		// Deserialize into StrippedHistoryEvents (should drop unknown fields)
		deserializedEvents, err := s.serializer.DeserializeStrippedEvents(dataBlob)
		s.NoError(err)
		s.Require().Len(deserializedEvents, 1)

		// Known fields should be preserved
		s.EqualValues(123, deserializedEvents[0].EventId)
		s.EqualValues(456, deserializedEvents[0].Version)

		reflectMsg := deserializedEvents[0].ProtoReflect()
		s.Empty(reflectMsg.GetUnknown(), "Unknown fields should have been discarded")
	})
}

func (s *temporalSerializerSuite) TestSerializeWorkflowExecutionState() {
	state := &persistencespb.WorkflowExecutionState{
		RequestIds: make(map[string]*persistencespb.RequestIDInfo),
	}
	err := fakedata.FakeStruct(state)
	s.NoError(err)

	blob, err := s.serializer.WorkflowExecutionStateToBlob(state)
	s.NoError(err)

	deserializedState, err := s.serializer.WorkflowExecutionStateFromBlob(blob)
	s.NoError(err)
	s.NotNil(deserializedState)

	// Deserialization adds the CreateRequestId to the Details.RequestIds map.
	state.RequestIds[state.CreateRequestId] = &persistencespb.RequestIDInfo{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		EventId:   common.FirstEventID,
	}
	s.ProtoEqual(state, deserializedState)

	blob, err = s.serializer.WorkflowExecutionStateToBlob(state)
	s.NoError(err)

	deserializedState, err = s.serializer.WorkflowExecutionStateFromBlob(blob)
	s.NoError(err)
	s.NotNil(deserializedState)
	s.ProtoEqual(state, deserializedState)
}

// HistoryService returns a different GetWorkflowExecutionHistoryResponse GetWorkflowExecutionHistoryResponseWithRaw to
// WorkflowHandler. Since HistoryClient is defined with the response type GetWorkflowExecutionHistoryResponse, grpc
// will deserialize this message to GetWorkflowExecutionHistoryResponse. This is done to avoid the extra CPU usage in
// history service to deserialize event blobs to []*HistoryEvent. This test ensures that
// GetWorkflowExecutionHistoryResponseWithRaw is correctly deserialized to GetWorkflowExecutionHistoryResponse.
func (s *temporalSerializerSuite) TestGetWorkflowExecutionHistoryResponseWithRawHistoryEvents() {
	// Create history events and batches
	fullHistory := &historypb.History{
		Events: []*historypb.HistoryEvent{
			{
				EventId:   1,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				Version:   100,
			},
			{
				EventId:   2,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				Version:   101,
			},
			{
				EventId:   3,
				EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
				Version:   102,
			},
			{
				EventId:   4,
				EventType: enumspb.EVENT_TYPE_TIMER_FIRED,
				Version:   103,
			},
		},
	}

	batch1 := &historypb.History{
		Events: fullHistory.Events[:2],
	}

	batch2 := &historypb.History{
		Events: fullHistory.Events[2:],
	}

	// Marshal each batch
	rawHistory1, err := batch1.Marshal()
	s.Require().NoError(err)

	db1 := &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         rawHistory1,
	}

	rawHistory2, err := batch2.Marshal()
	s.Require().NoError(err)

	db2 := &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         rawHistory2,
	}

	rawResp := &historyservice.GetWorkflowExecutionHistoryResponseWithRaw{
		History: [][]byte{db1.Data, db2.Data},
	}

	serializedRawResp, err := rawResp.Marshal()
	s.Require().NoError(err)

	resp := &historyservice.GetWorkflowExecutionHistoryResponse{}
	err = resp.Unmarshal(serializedRawResp)
	s.Require().NoError(err)

	// Verify resp has same list of history events as fullHistory
	for i, event := range resp.History.Events {
		s.Equal(fullHistory.Events[i].EventId, event.EventId)
		s.Equal(fullHistory.Events[i].Version, event.Version)
		s.Equal(fullHistory.Events[i].EventType, event.EventType)
		s.Nil(event.Attributes)
	}
}
