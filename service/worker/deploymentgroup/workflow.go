// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2024 Uber Technologies, Inc.
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

package deploymentgroup

import (
	enumspb "go.temporal.io/api/enums/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	deploymentpb "go.temporal.io/server/api/deployment_group/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/service/history/workflow"
)

// Define types required by local state here
type (
	TaskQueue struct {
		Name          string
		TaskQueueType enumspb.TaskQueueType
	}
	Rollout struct {
		Status         enumsspb.DeploymentRolloutState
		InitiationTime *timestamppb.Timestamp // TODO Shivam - check if this is right?
		CompleteTime   *timestamppb.Timestamp
		CancelTime     *timestamppb.Timestamp
	}
	Deployment struct {
		BuildId   string
		Rollout   Rollout     // represents a state machine with multiple states
		TaskQueue []TaskQueue // All the task queues associated with this WD and buildID (key)
	}
	DeploymentGroup struct {
		Name       string
		Deployment []Deployment
	}
)

// The skeleton of the workflow shall go in here

func DeploymentWorkflow(ctx workflow.Context, deploymentWorkflowArgs deploymentpb.DeploymentWorkflowArgs) {
	// The local state of a deploymentWorkflow

	// In the even that the workflow was firstly created, the arguments supplied still contains the poller
	// information
	deployment := DeploymentGroup{
		Name:       deploymentWorkflowArgs.DeploymentName,
		Deployment: make([]Deployment, 0),
	}

	// Make a signal handler to handle signal of a certain type which shall make an individual deployment
	// for this deploymentGroup.

	/* to think about -
	having multiple signal handlers - do we want to use multiple signal channels or a selector?


	The current pattern I am thinking is:

	for {

		// wait for receiving signal/s. If the number of signals received crosses a threshold, break
		from the for loop, drain the signals async, and use CAN.

	}
	*/

}
