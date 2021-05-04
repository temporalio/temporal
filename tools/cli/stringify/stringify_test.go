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

package stringify

import (
	"strings"
	"testing"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/sdk/converter"

	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives/timestamp"
)

var (
	dataConverter = converter.GetDefaultDataConverter()
)

type stringifySuite struct {
	suite.Suite
}

func TestStringifySuite(t *testing.T) {
	s := &stringifySuite{}
	suite.Run(t, s)
}

func (s *stringifySuite) SetupSuite() {
}

func (s *stringifySuite) SetupTest() {
}

func (s *stringifySuite) TearDownTest() {
}

func (s *stringifySuite) TestBreakLongWords() {
	s.Equal("111 222 333 4", breakLongWords("1112223334", 3))
	s.Equal("111 2 223", breakLongWords("1112 223", 3))
	s.Equal("11 122 23", breakLongWords("11 12223", 3))
	s.Equal("111", breakLongWords("111", 3))
	s.Equal("", breakLongWords("", 3))
	s.Equal("111  222", breakLongWords("111 222", 3))
}

func (s *stringifySuite) TestAnyToString() {
	arg := strings.Repeat("LongText", 80)
	event := &historypb.HistoryEvent{
		EventId:   1,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
			WorkflowType:        &commonpb.WorkflowType{Name: "helloworldWorkflow"},
			TaskQueue:           &taskqueuepb.TaskQueue{Name: "taskQueue"},
			WorkflowRunTimeout:  timestamp.DurationPtr(60 * time.Second),
			WorkflowTaskTimeout: timestamp.DurationPtr(10 * time.Second),
			Identity:            "tester",
			Input:               payloads.EncodeString(arg),
		}},
	}
	res := AnyToString(event, false, 500, dataConverter)
	ss, l := tablewriter.WrapString(res, 10)
	s.Equal(6, len(ss))
	s.Equal(120, l)
}

func (s *stringifySuite) TestAnyToString_DecodeMapValues() {
	fields := map[string]*commonpb.Payload{
		"TestKey": payload.EncodeString("testValue"),
	}
	execution := &workflowpb.WorkflowExecutionInfo{
		Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		Memo:   &commonpb.Memo{Fields: fields},
	}
	s.Equal(`{Status:Running, HistoryLength:0, Memo:{Fields:map{TestKey:"testValue"}}, StateTransitionCount:0}`, AnyToString(execution, true, 0, dataConverter))

	fields["TestKey2"] = payload.EncodeString("anotherTestValue")
	execution.Memo = &commonpb.Memo{Fields: fields}
	got := AnyToString(execution, true, 0, dataConverter)
	expected := `{Status:Running, HistoryLength:0, Memo:{Fields:map{TestKey:"testValue", TestKey2:"anotherTestValue"}}, StateTransitionCount:0}`
	s.Equal(expected, got)
}

func (s *stringifySuite) TestAnyToString_Slice() {
	var fields []string
	got := AnyToString(fields, true, 0, dataConverter)
	s.Equal("[]", got)

	fields = make([]string, 0)
	got = AnyToString(fields, true, 0, dataConverter)
	s.Equal("[]", got)

	fields = make([]string, 1)
	got = AnyToString(fields, true, 0, dataConverter)
	s.Equal("[]", got)

	fields[0] = "qwe"
	got = AnyToString(fields, true, 0, dataConverter)
	s.Equal("[qwe]", got)
	got = AnyToString(fields, false, 0, dataConverter)
	s.Equal("[qwe]", got)

	fields = make([]string, 2)
	fields[0] = "asd"
	fields[1] = "zxc"
	got = AnyToString(fields, true, 0, dataConverter)
	s.Equal("[asd,zxc]", got)
	got = AnyToString(fields, false, 0, dataConverter)
	s.Equal("[asd,...1 more]", got)

	fields = make([]string, 3)
	fields[0] = "0"
	fields[1] = "1"
	fields[2] = "2"
	got = AnyToString(fields, true, 0, dataConverter)
	s.Equal("[0,1,2]", got)
	got = AnyToString(fields, false, 0, dataConverter)
	s.Equal("[0,...2 more]", got)

}

func (s *stringifySuite) TestIsAttributeName() {
	s.True(isAttributeName("WorkflowExecutionStartedEventAttributes"))
	s.False(isAttributeName("workflowExecutionStartedEventAttributes"))
}
