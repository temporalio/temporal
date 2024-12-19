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
	"context"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
)

type WorkflowUpdateBaseSuite struct {
	testcore.FunctionalSuite
}

type updateResponseErr struct {
	response *workflowservice.UpdateWorkflowExecutionResponse
	err      error
}

func (s *WorkflowUpdateBaseSuite) sendUpdateNoErrorWaitPolicyAccepted(tv *testvars.TestVars, updateID string) <-chan *workflowservice.UpdateWorkflowExecutionResponse {
	s.T().Helper()
	return s.sendUpdateNoErrorInternal(tv, updateID, &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED})
}

func (s *WorkflowUpdateBaseSuite) sendUpdateNoErrorInternal(tv *testvars.TestVars, updateID string, waitPolicy *updatepb.WaitPolicy) <-chan *workflowservice.UpdateWorkflowExecutionResponse {
	s.T().Helper()
	retCh := make(chan *workflowservice.UpdateWorkflowExecutionResponse)
	syncCh := make(chan struct{})
	go func() {
		urCh := s.sendUpdateInternal(testcore.NewContext(), tv, updateID, waitPolicy, true)
		// Unblock return only after the server admits update.
		syncCh <- struct{}{}
		// Unblocked when an update result is ready.
		retCh <- (<-urCh).response
	}()
	<-syncCh
	return retCh
}

func (s *WorkflowUpdateBaseSuite) sendUpdateInternal(
	ctx context.Context,
	tv *testvars.TestVars,
	updateID string,
	waitPolicy *updatepb.WaitPolicy,
	requireNoError bool,
) <-chan updateResponseErr {

	s.T().Helper()

	updateResultCh := make(chan updateResponseErr)
	go func() {
		updateResp, updateErr := s.FrontendClient().UpdateWorkflowExecution(ctx, s.updateWorkflowRequest(tv, waitPolicy, updateID))
		// It is important to do assert here (before writing to channel which doesn't have readers yet)
		// to fail fast without trying to process update in wtHandler.
		if requireNoError {
			require.NoError(s.T(), updateErr)
		}
		updateResultCh <- updateResponseErr{response: updateResp, err: updateErr}
	}()
	s.waitUpdateAdmitted(tv, updateID)
	return updateResultCh
}

func (s *WorkflowUpdateBaseSuite) updateWorkflowRequest(
	tv *testvars.TestVars,
	waitPolicy *updatepb.WaitPolicy,
	updateID string,
) *workflowservice.UpdateWorkflowExecutionRequest {
	return &workflowservice.UpdateWorkflowExecutionRequest{
		Namespace:         s.Namespace(),
		WorkflowExecution: tv.WorkflowExecution(),
		WaitPolicy:        waitPolicy,
		Request: &updatepb.Request{
			Meta: &updatepb.Meta{UpdateId: tv.UpdateID(updateID)},
			Input: &updatepb.Input{
				Name: tv.HandlerName(),
				Args: payloads.EncodeString("args-value-of-" + tv.UpdateID(updateID)),
			},
		},
	}
}

func (s *WorkflowUpdateBaseSuite) waitUpdateAdmitted(tv *testvars.TestVars, updateID string) {
	s.T().Helper()
	s.EventuallyWithTf(func(collect *assert.CollectT) {
		pollResp, pollErr := s.FrontendClient().PollWorkflowExecutionUpdate(testcore.NewContext(), &workflowservice.PollWorkflowExecutionUpdateRequest{
			Namespace:  s.Namespace(),
			UpdateRef:  tv.UpdateRef(updateID),
			WaitPolicy: &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED},
		})

		assert.NoError(collect, pollErr)
		assert.GreaterOrEqual(collect, pollResp.GetStage(), enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED)
	}, 5*time.Second, 10*time.Millisecond, "update %s did not reach Admitted stage", tv.UpdateID(updateID))
}

func (s *WorkflowUpdateBaseSuite) startWorkflow(tv *testvars.TestVars) *testvars.TestVars {
	s.T().Helper()
	startResp, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), s.startWorkflowRequest(tv))
	s.NoError(err)

	return tv.WithRunID(startResp.GetRunId())
}

func (s *WorkflowUpdateBaseSuite) startWorkflowRequest(tv *testvars.TestVars) *workflowservice.StartWorkflowExecutionRequest {
	return &workflowservice.StartWorkflowExecutionRequest{
		RequestId:    tv.Any().String(),
		Namespace:    s.Namespace(),
		WorkflowId:   tv.WorkflowID(),
		WorkflowType: tv.WorkflowType(),
		TaskQueue:    tv.TaskQueue(),
	}
}
