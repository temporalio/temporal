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

package update

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	protocolpb "go.temporal.io/api/protocol/v1"
	updatepb "go.temporal.io/api/update/v1"
)

type (
	updateSuite struct {
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
		suite.Suite
	}
)

func (s *updateSuite) SetupSuite() {
	s.Assertions = require.New(s.T())
}

func TestUpdateSuite(t *testing.T) {
	suite.Run(t, new(updateSuite))
}

func (s *updateSuite) TestValidateMessages() {

	reg := NewRegistry()
	upd1, _, _ := reg.Add(&updatepb.Request{Meta: &updatepb.Meta{UpdateId: "update-1"}})

	testCases := []struct {
		Name     string
		Error    string
		Messages []*protocolpb.Message
	}{
		{
			Name:  "update id not found",
			Error: "not found",
			Messages: []*protocolpb.Message{
				{
					Id:                 uuid.New(),
					ProtocolInstanceId: "bogus-update-id",
					SequencingId:       nil,
					Body:               marshalAny(s, &updatepb.Acceptance{}),
				},
			},
		},
		{
			Name:  "unknown message type",
			Error: "unknown message type",
			Messages: []*protocolpb.Message{
				{
					Id:                 uuid.New(),
					ProtocolInstanceId: upd1.protocolInstanceID,
					SequencingId:       nil,
					Body:               marshalAny(s, &updatepb.WaitPolicy{}),
				},
			},
		},
		{
			Name:  "duplicate acceptance",
			Error: "BadUpdateWorkflowExecutionMessage: failed to accept update update-1, current state: accepted",
			Messages: []*protocolpb.Message{
				{
					Id:                 uuid.New(),
					ProtocolInstanceId: upd1.protocolInstanceID,
					SequencingId:       nil,
					Body:               marshalAny(s, &updatepb.Acceptance{}),
				},
				{
					Id:                 uuid.New(),
					ProtocolInstanceId: upd1.protocolInstanceID,
					SequencingId:       nil,
					Body:               marshalAny(s, &updatepb.Acceptance{}),
				},
			},
		},
		{
			Name:  "complete without accept",
			Error: "BadUpdateWorkflowExecutionMessage: failed to complete update update-1, current state: pending",
			Messages: []*protocolpb.Message{
				{
					Id:                 uuid.New(),
					ProtocolInstanceId: upd1.protocolInstanceID,
					SequencingId:       nil,
					Body:               marshalAny(s, &updatepb.Response{}),
				},
			},
		},
		{
			Name:  "reject after accept",
			Error: "BadUpdateWorkflowExecutionMessage: failed to reject update update-1, current state: accepted",
			Messages: []*protocolpb.Message{
				{
					Id:                 uuid.New(),
					ProtocolInstanceId: upd1.protocolInstanceID,
					SequencingId:       nil,
					Body:               marshalAny(s, &updatepb.Acceptance{}),
				},
				{
					Id:                 uuid.New(),
					ProtocolInstanceId: upd1.protocolInstanceID,
					SequencingId:       nil,
					Body:               marshalAny(s, &updatepb.Rejection{}),
				},
			},
		},
		{
			Name:  "complete after reject",
			Error: "BadUpdateWorkflowExecutionMessage: failed to complete update update-1, current state: rejected",
			Messages: []*protocolpb.Message{
				{
					Id:                 uuid.New(),
					ProtocolInstanceId: upd1.protocolInstanceID,
					SequencingId:       nil,
					Body:               marshalAny(s, &updatepb.Rejection{}),
				},
				{
					Id:                 uuid.New(),
					ProtocolInstanceId: upd1.protocolInstanceID,
					SequencingId:       nil,
					Body:               marshalAny(s, &updatepb.Response{}),
				},
			},
		},

		{
			Name: "accept only",
			Messages: []*protocolpb.Message{
				{
					Id:                 uuid.New(),
					ProtocolInstanceId: upd1.protocolInstanceID,
					SequencingId:       nil,
					Body:               marshalAny(s, &updatepb.Acceptance{}),
				},
			},
		},
		{
			Name: "reject only",
			Messages: []*protocolpb.Message{
				{
					Id:                 uuid.New(),
					ProtocolInstanceId: upd1.protocolInstanceID,
					SequencingId:       nil,
					Body:               marshalAny(s, &updatepb.Rejection{}),
				},
			},
		},

		{
			Name: "accept and complete",
			Messages: []*protocolpb.Message{
				{
					Id:                 uuid.New(),
					ProtocolInstanceId: upd1.protocolInstanceID,
					SequencingId:       nil,
					Body:               marshalAny(s, &updatepb.Acceptance{}),
				},
				{
					Id:                 uuid.New(),
					ProtocolInstanceId: upd1.protocolInstanceID,
					SequencingId:       nil,
					Body:               marshalAny(s, &updatepb.Response{}),
				},
			},
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.Name, func(t *testing.T) {
			err := reg.ValidateIncomingMessages(tc.Messages)
			if len(tc.Error) > 0 {
				s.Error(err)
				s.Contains(err.Error(), tc.Error)
			} else {
				s.NoError(err)
			}
		})
	}
}

func marshalAny(s *updateSuite, pb proto.Message) *types.Any {
	s.T().Helper()
	a, err := types.MarshalAny(pb)
	s.NoError(err)
	return a
}
