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
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/suite"

	workflow "github.com/uber/cadence/.gen/go/shared"
)

type (
	thriftRWEncoderSuite struct {
		suite.Suite
		encoder *ThriftRWEncoder
	}
)

var (
	thriftObject = &workflow.HistoryEvent{
		Version:   int64Ptr(1234),
		EventId:   int64Ptr(130),
		Timestamp: int64Ptr(112345132134),
		EventType: workflow.EventTypeRequestCancelExternalWorkflowExecutionInitiated.Ptr(),
		RequestCancelExternalWorkflowExecutionInitiatedEventAttributes: &workflow.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{
			Domain: stringPtr("some random target domain"),
			WorkflowExecution: &workflow.WorkflowExecution{
				WorkflowId: stringPtr("some random target workflow ID"),
				RunId:      stringPtr("some random target run ID"),
			},
			ChildWorkflowOnly: boolPtr(true),
			Control:           []byte("some random control"),
		},
	}
	thriftEncodedBinary = []byte{
		89, 10, 0, 10, 0, 0, 0, 0, 0, 0, 0, 130, 10, 0, 20, 0, 0, 0, 26, 40, 74, 172, 102,
		8, 0, 30, 0, 0, 0, 23, 10, 0, 35, 0, 0, 0, 0, 0, 0, 4, 210, 12, 1, 44, 11, 0, 20,
		0, 0, 0, 25, 115, 111, 109, 101, 32, 114, 97, 110, 100, 111, 109, 32, 116, 97, 114,
		103, 101, 116, 32, 100, 111, 109, 97, 105, 110, 12, 0, 30, 11, 0, 10, 0, 0, 0, 30,
		115, 111, 109, 101, 32, 114, 97, 110, 100, 111, 109, 32, 116, 97, 114, 103, 101,
		116, 32, 119, 111, 114, 107, 102, 108, 111, 119, 32, 73, 68, 11, 0, 20, 0, 0, 0, 25,
		115, 111, 109, 101, 32, 114, 97, 110, 100, 111, 109, 32, 116, 97, 114, 103, 101, 116,
		32, 114, 117, 110, 32, 73, 68, 0, 11, 0, 40, 0, 0, 0, 19, 115, 111, 109, 101, 32, 114,
		97, 110, 100, 111, 109, 32, 99, 111, 110, 116, 114, 111, 108, 2, 0, 50, 1, 0, 0,
	}
)

func TestThriftRWEncoderSuite(t *testing.T) {
	s := new(thriftRWEncoderSuite)
	suite.Run(t, s)
}

func (s *thriftRWEncoderSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}
	s.encoder = NewThriftRWEncoder()
}

func (s *thriftRWEncoderSuite) TestEncode() {
	binary, err := s.encoder.Encode(thriftObject)
	s.Nil(err)
	s.Equal(thriftEncodedBinary, binary)
}

func (s *thriftRWEncoderSuite) TestDecode() {
	var val workflow.HistoryEvent
	err := s.encoder.Decode(thriftEncodedBinary, &val)
	s.Nil(err)
	val.Equals(thriftObject)
}

func (s *thriftRWEncoderSuite) TestDecode_MissingVersion() {
	binary := []byte{}
	var val workflow.HistoryEvent
	err := s.encoder.Decode(binary, &val)
	s.Equal(MissingBinaryEncodingVersion, err)
}

func (s *thriftRWEncoderSuite) TestDecode_InvalidVersion() {
	binary := []byte{}
	binary = append(binary, thriftEncodedBinary...)
	binary[0] = preambleVersion0 - 1

	var val workflow.HistoryEvent
	err := s.encoder.Decode(binary, &val)
	s.Equal(InvalidBinaryEncodingVersion, err)
}

func int64Ptr(v int64) *int64 {
	return &v
}

func stringPtr(v string) *string {
	return &v
}

func boolPtr(v bool) *bool {
	return &v
}
