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
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	updatepb "go.temporal.io/api/update/v1"
)

type (
	// Status describes current status of update from registry.
	Status struct {
		// The most advanced stage reached by update.
		Stage enumspb.UpdateWorkflowExecutionLifecycleStage
		// Outcome of update if it is completed or rejected.
		Outcome *updatepb.Outcome
	}
)

func statusAdmitted() *Status {
	return &Status{
		Stage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED,
	}
}

func statusAccepted() *Status {
	return &Status{
		Stage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED,
	}
}

func statusRejected(rejection *failurepb.Failure) *Status {
	return &Status{
		Stage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED,
		Outcome: &updatepb.Outcome{
			Value: &updatepb.Outcome_Failure{Failure: rejection},
		},
	}
}

func statusCompleted(outcome *updatepb.Outcome) *Status {
	return &Status{
		Stage:   enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED,
		Outcome: outcome,
	}
}
