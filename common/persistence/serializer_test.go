package persistence

import (
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
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	temporalSerializerSuite struct {
		suite.Suite
		// not merely log an error
		protorequire.ProtoAssertions
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
}

func (s *temporalSerializerSuite) TestSerializer() {

	concurrency := 1
	startWG := sync.WaitGroup{}
	doneWG := sync.WaitGroup{}

	startWG.Add(1)
	doneWG.Add(concurrency)

	serializer := serialization.NewSerializer()

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

			nilEvent, err := serializer.SerializeEvent(nil)
			require.Nil(s.T(), err)
			require.Nil(s.T(), nilEvent)

			dProto, err := serializer.SerializeEvent(event0)
			require.Nil(s.T(), err)
			require.NotNil(s.T(), dProto)

			// serialize batch events

			nilEvents, err := serializer.SerializeEvents(nil)
			require.Nil(s.T(), err)
			require.NotNil(s.T(), nilEvents)

			dsProto, err := serializer.SerializeEvents(history0.Events)
			require.Nil(s.T(), err)
			require.NotNil(s.T(), dsProto)

			// deserialize event

			dNilEvent, err := serializer.DeserializeEvent(nilEvent)
			require.Nil(s.T(), err)
			require.Nil(s.T(), dNilEvent)

			event2, err := serializer.DeserializeEvent(dProto)
			require.Nil(s.T(), err)
			s.ProtoEqual(event0, event2)

			// deserialize events

			dNilEvents, err := serializer.DeserializeEvents(nilEvents)
			require.Nil(s.T(), err)
			require.Nil(s.T(), dNilEvents)

			events, err := serializer.DeserializeEvents(dsProto)
			history2 := &historypb.History{Events: events}
			require.Nil(s.T(), err)
			s.ProtoEqual(history0, history2)
		}()
	}

	startWG.Done()
	succ := common.AwaitWaitGroup(&doneWG, 10*time.Second)
	require.True(s.T(), succ, "test timed out")
}
