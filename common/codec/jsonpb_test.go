package codec

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/common/testing/protoassert"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	jsonpbEncoderSuite struct {
		suite.Suite
		encoder JSONPBEncoder
	}
)

var (
	history = &historypb.History{
		Events: []*historypb.HistoryEvent{
			{
				Version:   1234,
				EventId:   130,
				EventTime: timestamppb.New(time.Date(1978, 8, 22, 0, 0, 0, 0, time.UTC)),
				EventType: enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
				Attributes: &historypb.HistoryEvent_RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{RequestCancelExternalWorkflowExecutionInitiatedEventAttributes: &historypb.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{
					Namespace: "some random target namespace",
					WorkflowExecution: &commonpb.WorkflowExecution{
						WorkflowId: "some random target workflow ID",
						RunId:      "some random target run ID",
					},
					ChildWorkflowOnly: true,
					Control:           "some random control",
				}},
			},
		},
	}
	encodedHistory    = `{"events": [{"eventId":"130","eventTime":"1978-08-22T00:00:00Z","eventType":"EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED","version":"1234","requestCancelExternalWorkflowExecutionInitiatedEventAttributes":{"namespace":"some random target namespace","workflowExecution":{"workflowId":"some random target workflow ID","runId":"some random target run ID"},"control":"some random control","childWorkflowOnly":true}}]}`
	oldEncodedHistory = `{"events": [{"eventId":"130","eventTime":"1978-08-22T00:00:00Z","eventType":"RequestCancelExternalWorkflowExecutionInitiated","version":"1234","requestCancelExternalWorkflowExecutionInitiatedEventAttributes":{"namespace":"some random target namespace","workflowExecution":{"workflowId":"some random target workflow ID","runId":"some random target run ID"},"control":"some random control","childWorkflowOnly":true}}]}`
)

func TestJSONPBEncoderSuite(t *testing.T) {
	s := new(jsonpbEncoderSuite)
	suite.Run(t, s)
}

func (s *jsonpbEncoderSuite) SetupSuite() {
	s.encoder = NewJSONPBEncoder()
}

func (s *jsonpbEncoderSuite) TestEncode() {
	json, err := s.encoder.Encode(history)
	s.Require().NoError(err)
	s.JSONEq(encodedHistory, string(json))
}

func (s *jsonpbEncoderSuite) TestDecode() {
	var val historypb.History
	err := s.encoder.Decode([]byte(encodedHistory), &val)
	s.Require().NoError(err)
	protoassert.ProtoEqual(s.T(), &val, history)
}

func (s *jsonpbEncoderSuite) TestEncodeHistories() {
	var histories []*historypb.History
	histories = append(histories, history)
	histories = append(histories, history)
	histories = append(histories, history)

	json, err := s.encoder.EncodeHistories(histories)
	s.Require().NoError(err)
	s.JSONEq(fmt.Sprintf("[%[1]s,%[1]s,%[1]s]", encodedHistory), string(json))
}

func (s *jsonpbEncoderSuite) TestEncodeEmptyHistories() {
	var histories []*historypb.History

	json, err := s.encoder.EncodeHistories(histories)
	s.Require().NoError(err)
	s.JSONEq("[]", string(json))
}

func (s *jsonpbEncoderSuite) TestDecodeHistories() {
	historyJSON := fmt.Sprintf("[%[1]s,%[1]s,%[1]s]", encodedHistory)

	var histories []*historypb.History
	histories = append(histories, history)
	histories = append(histories, history)
	histories = append(histories, history)

	decodedHistories, err := s.encoder.DecodeHistories([]byte(historyJSON))

	s.Require().NoError(err)
	protoassert.ProtoSliceEqual(s.T(), histories, decodedHistories)
}

func (s *jsonpbEncoderSuite) TestDecodeOldHistories() {
	historyJSON := fmt.Sprintf("[%[1]s,%[1]s,%[1]s]", oldEncodedHistory)

	var historyEvents []*historypb.History
	historyEvents = append(historyEvents, history)
	historyEvents = append(historyEvents, history)
	historyEvents = append(historyEvents, history)

	decodedHistories, err := s.encoder.DecodeHistories([]byte(historyJSON))

	s.Require().NoError(err)
	protoassert.ProtoSliceEqual(s.T(), historyEvents, decodedHistories)
}
