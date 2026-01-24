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
	event0 := historypb.HistoryEvent_builder{
		EventId:   999,
		EventTime: timestamppb.New(time.Date(2020, 8, 22, 0, 0, 0, 0, time.UTC)),
		EventType: eventType,
		ActivityTaskCompletedEventAttributes: historypb.ActivityTaskCompletedEventAttributes_builder{
			Result:           payloads.EncodeString("result-1-event-1"),
			ScheduledEventId: 4,
			StartedEventId:   5,
			Identity:         "event-1",
		}.Build(),
	}.Build()

	history0 := historypb.History_builder{Events: []*historypb.HistoryEvent{event0, event0}}.Build()

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

			dsProto, err := s.serializer.SerializeEvents(history0.GetEvents())
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
			history2 := historypb.History_builder{Events: events}.Build()
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

	shardInfo.SetShardId(rand.Int31())
	shardInfo.SetRangeId(rand.Int63())

	categoryID := rand.Int31()
	shardInfo.SetQueueStates(make(map[int32]*persistencespb.QueueState))
	shardInfo.GetQueueStates()[categoryID] = persistencespb.QueueState_builder{
		ReaderStates: make(map[int64]*persistencespb.QueueReaderState),
	}.Build()
	shardInfo.GetQueueStates()[categoryID].GetReaderStates()[rand.Int63()] = persistencespb.QueueReaderState_builder{
		Scopes: make([]*persistencespb.QueueSliceScope, 0),
	}.Build()
	shardInfo.SetReplicationDlqAckLevel(make(map[string]int64))

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
		events, err := s.serializer.DeserializeStrippedEvents(commonpb.DataBlob_builder{
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			Data:         nil,
		}.Build())
		s.NoError(err)
		s.Nil(events)

		// Data is empty byte array
		events, err = s.serializer.DeserializeStrippedEvents(commonpb.DataBlob_builder{
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			Data:         []byte{},
		}.Build())
		s.NoError(err)
		s.Nil(events)
	})

	// 3. Unknown encoding type
	s.Run("UnknownEncodingType", func() {
		_, err := s.serializer.DeserializeStrippedEvents(commonpb.DataBlob_builder{
			EncodingType: enumspb.ENCODING_TYPE_UNSPECIFIED,
			Data:         []byte("irrelevant-data"),
		}.Build())
		s.Error(err)
		s.Contains(err.Error(), "unknown or unsupported encoding type")
	})

	// 4. Proper proto decoding, discarding unknown fields
	s.Run("ProtoDiscardUnknownFields", func() {
		// Build a HistoryEvent that contains fields *not* present in StrippedHistoryEvent
		historyEvent := historypb.HistoryEvent_builder{
			EventId:   123,
			Version:   456,
			EventTime: nil, // or a valid timestamp
			// This is an extra field not present in StrippedHistoryEvent
			WorkflowExecutionStartedEventAttributes: historypb.WorkflowExecutionStartedEventAttributes_builder{
				WorkflowType: commonpb.WorkflowType_builder{Name: "some-workflow-type"}.Build(),
			}.Build(),
		}.Build()

		historyEvents := historypb.History_builder{
			Events: []*historypb.HistoryEvent{historyEvent},
		}.Build()

		// Marshal to protobuf
		data, err := historyEvents.Marshal()
		s.Require().NoError(err)

		dataBlob := commonpb.DataBlob_builder{
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			Data:         data,
		}.Build()

		// Deserialize into StrippedHistoryEvents (should drop unknown fields)
		deserializedEvents, err := s.serializer.DeserializeStrippedEvents(dataBlob)
		s.NoError(err)
		s.Require().Len(deserializedEvents, 1)

		// Known fields should be preserved
		s.EqualValues(123, deserializedEvents[0].GetEventId())
		s.EqualValues(456, deserializedEvents[0].GetVersion())

		reflectMsg := deserializedEvents[0].ProtoReflect()
		s.Empty(reflectMsg.GetUnknown(), "Unknown fields should have been discarded")
	})
}

func (s *temporalSerializerSuite) TestSerializeWorkflowExecutionState() {
	state := persistencespb.WorkflowExecutionState_builder{
		RequestIds: make(map[string]*persistencespb.RequestIDInfo),
	}.Build()
	err := fakedata.FakeStruct(state)
	s.NoError(err)

	blob, err := s.serializer.WorkflowExecutionStateToBlob(state)
	s.NoError(err)

	deserializedState, err := s.serializer.WorkflowExecutionStateFromBlob(blob)
	s.NoError(err)
	s.NotNil(deserializedState)

	// Deserialization adds the CreateRequestId to the Details.RequestIds map.
	state.GetRequestIds()[state.GetCreateRequestId()] = persistencespb.RequestIDInfo_builder{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		EventId:   common.FirstEventID,
	}.Build()
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
	fullHistory := historypb.History_builder{
		Events: []*historypb.HistoryEvent{
			historypb.HistoryEvent_builder{
				EventId:   1,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				Version:   100,
			}.Build(),
			historypb.HistoryEvent_builder{
				EventId:   2,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				Version:   101,
			}.Build(),
			historypb.HistoryEvent_builder{
				EventId:   3,
				EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
				Version:   102,
			}.Build(),
			historypb.HistoryEvent_builder{
				EventId:   4,
				EventType: enumspb.EVENT_TYPE_TIMER_FIRED,
				Version:   103,
			}.Build(),
		},
	}.Build()

	batch1 := historypb.History_builder{
		Events: fullHistory.GetEvents()[:2],
	}.Build()

	batch2 := historypb.History_builder{
		Events: fullHistory.GetEvents()[2:],
	}.Build()

	// Marshal each batch
	rawHistory1, err := batch1.Marshal()
	s.Require().NoError(err)

	db1 := commonpb.DataBlob_builder{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         rawHistory1,
	}.Build()

	rawHistory2, err := batch2.Marshal()
	s.Require().NoError(err)

	db2 := commonpb.DataBlob_builder{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         rawHistory2,
	}.Build()

	rawResp := historyservice.GetWorkflowExecutionHistoryResponseWithRaw_builder{
		History: [][]byte{db1.GetData(), db2.GetData()},
	}.Build()

	serializedRawResp, err := rawResp.Marshal()
	s.Require().NoError(err)

	resp := &historyservice.GetWorkflowExecutionHistoryResponse{}
	err = resp.Unmarshal(serializedRawResp)
	s.Require().NoError(err)

	// Verify resp has same list of history events as fullHistory
	for i, event := range resp.GetHistory().GetEvents() {
		s.Equal(fullHistory.GetEvents()[i].GetEventId(), event.GetEventId())
		s.Equal(fullHistory.GetEvents()[i].GetVersion(), event.GetVersion())
		s.Equal(fullHistory.GetEvents()[i].GetEventType(), event.GetEventType())
		s.Nil(event.Attributes)
	}
}
