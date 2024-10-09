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

package tests

import (
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
)

type UserMetadataSuite struct {
	testcore.FunctionalSuite
}

func TestUserMetadataSuite(t *testing.T) {
	suite.Run(t, new(UserMetadataSuite))
}

func (s *UserMetadataSuite) TestUserMetadata() {
	getDescribeWorkflowExecutionInfo := func(client workflowservice.WorkflowServiceClient, namespace string, workflowID string, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
		return client.DescribeWorkflowExecution(testcore.NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
		})
	}
	prepareTestUserMetadata := func() *sdkpb.UserMetadata {
		return &sdkpb.UserMetadata{
			Summary: &commonpb.Payload{
				Metadata: map[string][]byte{"test_summary_key": []byte(`test_summary_val`)},
				Data:     []byte(`Test summary Data`),
			},
			Details: &commonpb.Payload{
				Metadata: map[string][]byte{"test_details_key": []byte(`test_details_val`)},
				Data:     []byte(`Test Details Data`),
			},
		}
	}

	s.Run("StartWorkflowExecution records UserMetadata", func() {
		tv := testvars.New(s.T())
		id := tv.WorkflowID("functional-user-metadata-StartWorkflowExecution")
		metadata := prepareTestUserMetadata()
		request := &workflowservice.StartWorkflowExecutionRequest{
			RequestId:    uuid.New(),
			Namespace:    s.Namespace(),
			WorkflowId:   id,
			WorkflowType: tv.WorkflowType(),
			TaskQueue:    tv.TaskQueue(),
			UserMetadata: metadata,
		}

		we, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
		s.NoError(err)

		// Verify that the UserMetadata associated with the start event is returned in the describe response.
		describeInfo, err := getDescribeWorkflowExecutionInfo(s.FrontendClient(), s.Namespace(), id, we.RunId)
		s.NoError(err)
		s.EqualExportedValues(metadata, describeInfo.ExecutionConfig.UserMetadata)
	})

	s.Run("SignalWithStartWorkflowExecution records UserMetadata", func() {
		tv := testvars.New(s.T())
		id := tv.WorkflowID("functional-user-metadata-SignalWithStartWorkflowExecution")
		metadata := prepareTestUserMetadata()
		request := &workflowservice.SignalWithStartWorkflowExecutionRequest{
			RequestId:    uuid.New(),
			Namespace:    s.Namespace(),
			WorkflowId:   id,
			WorkflowType: tv.WorkflowType(),
			TaskQueue:    tv.TaskQueue(),
			SignalName:   "TEST-SIGNAL",
			UserMetadata: metadata,
		}

		we, err := s.FrontendClient().SignalWithStartWorkflowExecution(testcore.NewContext(), request)
		s.NoError(err)

		// Verify that the UserMetadata associated with the start event is returned in the describe response.
		describeInfo, err := getDescribeWorkflowExecutionInfo(s.FrontendClient(), s.Namespace(), id, we.RunId)
		s.NoError(err)
		s.EqualExportedValues(metadata, describeInfo.ExecutionConfig.UserMetadata)
	})

	s.Run("ExecuteMultiOperation records UserMetadata", func() {
		tv := testvars.New(s.T())
		id := tv.WorkflowID("functional-user-metadata-ExecuteMultiOperation")
		metadata := prepareTestUserMetadata()
		startWorkflowRequest := &workflowservice.StartWorkflowExecutionRequest{
			RequestId:    uuid.New(),
			Namespace:    s.Namespace(),
			WorkflowId:   id,
			WorkflowType: tv.WorkflowType(),
			TaskQueue:    tv.TaskQueue(),
			UserMetadata: metadata,
		}
		updateWorkflowRequest := &workflowservice.UpdateWorkflowExecutionRequest{
			Namespace:         s.Namespace(),
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: id},
			Request: &updatepb.Request{
				Meta:  &updatepb.Meta{UpdateId: "UPDATE_ID"},
				Input: &updatepb.Input{Name: "NAME"},
			},
		}
		request := &workflowservice.ExecuteMultiOperationRequest{
			Namespace: s.Namespace(),
			Operations: []*workflowservice.ExecuteMultiOperationRequest_Operation{
				{ // start workflow operation
					Operation: &workflowservice.ExecuteMultiOperationRequest_Operation_StartWorkflow{
						StartWorkflow: startWorkflowRequest,
					},
				},
				{ // update workflow operation
					Operation: &workflowservice.ExecuteMultiOperationRequest_Operation_UpdateWorkflow{
						UpdateWorkflow: updateWorkflowRequest,
					},
				},
			},
		}

		_, err := s.FrontendClient().ExecuteMultiOperation(testcore.NewContext(), request)
		s.NoError(err)

		// Verify that the UserMetadata associated with the start event is returned in the describe response.
		describeInfo, err := getDescribeWorkflowExecutionInfo(s.FrontendClient(), s.Namespace(), id, "")
		s.NoError(err)
		s.EqualExportedValues(metadata, describeInfo.ExecutionConfig.UserMetadata)
	})

}
