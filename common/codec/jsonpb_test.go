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

package codec

import (
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/temporal-proto/common"
	decisionpb "go.temporal.io/temporal-proto/decision"
	eventpb "go.temporal.io/temporal-proto/event"
	executionpb "go.temporal.io/temporal-proto/execution"
	filterpb "go.temporal.io/temporal-proto/filter"
	namespacepb "go.temporal.io/temporal-proto/namespace"
	querypb "go.temporal.io/temporal-proto/query"
	tasklistpb "go.temporal.io/temporal-proto/tasklist"
	versionpb "go.temporal.io/temporal-proto/version"
	)

type (
	jsonpbEncoderSuite struct {
		suite.Suite
		encoder *JSONPBEncoder
	}
)

var (
	historyEvent = &historypb.HistoryEvent{
		Version:   1234,
		EventId:   130,
		Timestamp: 112345132134,
		EventType: eventpb.EventTypeRequestCancelExternalWorkflowExecutionInitiated,
		Attributes: &historypb.HistoryEvent_RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{RequestCancelExternalWorkflowExecutionInitiatedEventAttributes: &historypb.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{
			Namespace: "some random target namespace",
			WorkflowExecution: &executionpb.WorkflowExecution{
				WorkflowId: "some random target workflow ID",
				RunId:      "some random target run ID",
			},
			ChildWorkflowOnly: true,
			Control:           []byte("some random control"),
		}},
	}
	encodedHistoryEvent = `{"eventId":"130","timestamp":"112345132134","eventType":"EventTypeRequestCancelExternalWorkflowExecutionInitiated","version":"1234","requestCancelExternalWorkflowExecutionInitiatedEventAttributes":{"namespace":"some random target namespace","workflowExecution":{"workflowId":"some random target workflow ID","runId":"some random target run ID"},"control":"c29tZSByYW5kb20gY29udHJvbA==","childWorkflowOnly":true}}`
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
	var val historypb.HistoryEvent
	err := s.encoder.Decode([]byte(encodedHistoryEvent), &val)
	s.Nil(err)
	s.EqualValues(val, *historyEvent)
}

func (s *jsonpbEncoderSuite) TestEncodeSlice() {
	var historyEvents []*historypb.HistoryEvent
	historyEvents = append(historyEvents, historyEvent)
	historyEvents = append(historyEvents, historyEvent)
	historyEvents = append(historyEvents, historyEvent)

	json, err := s.encoder.EncodeHistoryEvents(historyEvents)
	s.Nil(err)
	s.Equal(fmt.Sprintf("[%[1]s,%[1]s,%[1]s]", encodedHistoryEvent), string(json))
}

func (s *jsonpbEncoderSuite) TestDecodeSlice() {
	historyEventsJSON := fmt.Sprintf("[%[1]s,%[1]s,%[1]s]", encodedHistoryEvent)

	var historyEvents []*historypb.HistoryEvent
	historyEvents = append(historyEvents, historyEvent)
	historyEvents = append(historyEvents, historyEvent)
	historyEvents = append(historyEvents, historyEvent)

	decodedHistoryEvents, err := s.encoder.DecodeHistoryEvents([]byte(historyEventsJSON))

	s.Nil(err)
	s.Equal(historyEvents, decodedHistoryEvents)
}
