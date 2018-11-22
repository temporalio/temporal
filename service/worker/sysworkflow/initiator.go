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

package sysworkflow

import (
	"context"
	"fmt"
	"github.com/uber/cadence/client/frontend"
	"github.com/uber/cadence/common/service/dynamicconfig"
	"go.uber.org/cadence/client"
	"math/rand"
)

type (

	// Initiator is used to trigger system tasks
	Initiator interface {
		Archive(request *ArchiveRequest) error
	}

	initiator struct {
		cadenceClient client.Client
		numSWFn       dynamicconfig.IntPropertyFn
	}

	// Signal is the data sent to system tasks
	Signal struct {
		RequestType    RequestType
		ArchiveRequest *ArchiveRequest
	}

	// ArchiveRequest signal used for archival task
	ArchiveRequest struct {
		Domain         string
		UserWorkflowID string
		UserRunID      string
	}
)

// NewInitiator creates a new Initiator
func NewInitiator(frontendClient frontend.Client, numSWFn dynamicconfig.IntPropertyFn) Initiator {
	return &initiator{
		cadenceClient: client.NewClient(frontendClient, Domain, &client.Options{}),
		numSWFn:       numSWFn,
	}
}

// Archive starts an archival task
func (i *initiator) Archive(request *ArchiveRequest) error {
	if request.Domain == Domain {
		return nil
	}
	workflowID := fmt.Sprintf("%v-%v", WorkflowIDPrefix, rand.Intn(i.numSWFn()))
	workflowOptions := client.StartWorkflowOptions{
		ID: workflowID,
		// TODO: once we have higher load, this should select one random of X task lists to do load balancing
		TaskList:                        DecisionTaskList,
		ExecutionStartToCloseTimeout:    WorkflowStartToCloseTimeout,
		DecisionTaskStartToCloseTimeout: DecisionTaskStartToCloseTimeout,
		WorkflowIDReusePolicy:           client.WorkflowIDReusePolicyAllowDuplicate,
	}
	signal := Signal{
		RequestType:    ArchivalRequest,
		ArchiveRequest: request,
	}

	_, err := i.cadenceClient.SignalWithStartWorkflow(
		context.Background(),
		workflowID,
		SignalName,
		signal,
		workflowOptions,
		SystemWorkflowFnName,
	)

	return err
}
