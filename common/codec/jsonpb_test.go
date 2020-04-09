package codec

import (
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/suite"
	eventpb "go.temporal.io/temporal-proto/event"
	executionpb "go.temporal.io/temporal-proto/execution"
)

type (
	jsonpbEncoderSuite struct {
		suite.Suite
		encoder *JSONPBEncoder
	}
)

var (
	historyEvent = &eventpb.HistoryEvent{
		Version:   1234,
		EventId:   130,
		Timestamp: 112345132134,
		EventType: eventpb.EventType_RequestCancelExternalWorkflowExecutionInitiated,
		Attributes: &eventpb.HistoryEvent_RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{RequestCancelExternalWorkflowExecutionInitiatedEventAttributes: &eventpb.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{
			Namespace: "some random target namespace",
			WorkflowExecution: &executionpb.WorkflowExecution{
				WorkflowId: "some random target workflow ID",
				RunId:      "some random target run ID",
			},
			ChildWorkflowOnly: true,
			Control:           []byte("some random control"),
		}},
	}
	encodedHistoryEvent = `{"eventId":"130","timestamp":"112345132134","eventType":"RequestCancelExternalWorkflowExecutionInitiated","version":"1234","requestCancelExternalWorkflowExecutionInitiatedEventAttributes":{"namespace":"some random target namespace","workflowExecution":{"workflowId":"some random target workflow ID","runId":"some random target run ID"},"control":"c29tZSByYW5kb20gY29udHJvbA==","childWorkflowOnly":true}}`
)

func TestJSONPBEncoderSuite(t *testing.T) {
	s := new(jsonpbEncoderSuite)
	suite.Run(t, s)
}

func (s *jsonpbEncoderSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}
	s.encoder = NewJSONPBEncoder()
}

func (s *jsonpbEncoderSuite) TestEncode() {
	json, err := s.encoder.Encode(historyEvent)
	s.Nil(err)
	s.Equal(encodedHistoryEvent, string(json))
}

func (s *jsonpbEncoderSuite) TestDecode() {
	var val eventpb.HistoryEvent
	err := s.encoder.Decode([]byte(encodedHistoryEvent), &val)
	s.Nil(err)
	s.EqualValues(val, *historyEvent)
}

func (s *jsonpbEncoderSuite) TestEncodeSlice() {
	var historyEvents []*eventpb.HistoryEvent
	historyEvents = append(historyEvents, historyEvent)
	historyEvents = append(historyEvents, historyEvent)
	historyEvents = append(historyEvents, historyEvent)

	json, err := s.encoder.EncodeHistoryEvents(historyEvents)
	s.Nil(err)
	s.Equal(fmt.Sprintf("[%[1]s,%[1]s,%[1]s]", encodedHistoryEvent), string(json))
}

func (s *jsonpbEncoderSuite) TestDecodeSlice() {
	historyEventsJSON := fmt.Sprintf("[%[1]s,%[1]s,%[1]s]", encodedHistoryEvent)

	var historyEvents []*eventpb.HistoryEvent
	historyEvents = append(historyEvents, historyEvent)
	historyEvents = append(historyEvents, historyEvent)
	historyEvents = append(historyEvents, historyEvent)

	decodedHistoryEvents, err := s.encoder.DecodeHistoryEvents([]byte(historyEventsJSON))

	s.Nil(err)
	s.Equal(historyEvents, decodedHistoryEvents)
}
