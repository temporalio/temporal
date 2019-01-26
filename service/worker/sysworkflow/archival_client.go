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
	"errors"
	"fmt"
	"github.com/uber/cadence/client/public"
	"github.com/uber/cadence/common/service/dynamicconfig"
	"go.uber.org/cadence/client"
	"math/rand"
)

type (
	// ArchiveRequest is request to Archive
	ArchiveRequest struct {
		DomainName string
		DomainID   string
		WorkflowID string
		RunID      string
		Bucket     string
	}

	// BackfillRequest is request to Backfill
	BackfillRequest struct {
		// TODO: fill out any fields needed for backfill
	}

	// ArchivalClient is used to archive workflow histories
	ArchivalClient interface {
		Archive(*ArchiveRequest) error
		Backfill(*BackfillRequest) error
	}

	archivalClient struct {
		cadenceClient client.Client
		numSWFn       dynamicconfig.IntPropertyFn
	}

	signal struct {
		RequestType    RequestType
		ArchiveRequest *ArchiveRequest
		BackillRequest *BackfillRequest
	}
)

// NewArchivalClient creates a new ArchivalClient
func NewArchivalClient(publicClient public.Client, numSWFn dynamicconfig.IntPropertyFn) ArchivalClient {
	return &archivalClient{
		cadenceClient: client.NewClient(publicClient, Domain, &client.Options{}),
		numSWFn:       numSWFn,
	}
}

// Archive starts an archival task
func (c *archivalClient) Archive(request *ArchiveRequest) error {
	if request.DomainName == Domain {
		return nil
	}
	workflowID := fmt.Sprintf("%v-%v", WorkflowIDPrefix, rand.Intn(c.numSWFn()))
	workflowOptions := client.StartWorkflowOptions{
		ID: workflowID,
		// TODO: once we have higher load, this should select one random of X task lists to do load balancing
		TaskList:                        DecisionTaskList,
		ExecutionStartToCloseTimeout:    WorkflowStartToCloseTimeout,
		DecisionTaskStartToCloseTimeout: DecisionTaskStartToCloseTimeout,
		WorkflowIDReusePolicy:           client.WorkflowIDReusePolicyAllowDuplicate,
	}
	signal := signal{
		RequestType:    archivalRequest,
		ArchiveRequest: request,
	}

	_, err := c.cadenceClient.SignalWithStartWorkflow(
		context.Background(),
		workflowID,
		SignalName,
		signal,
		workflowOptions,
		SystemWorkflowFnName,
	)

	return err
}

// Backfill starts a backfill task
func (c *archivalClient) Backfill(request *BackfillRequest) error {
	// TODO: implement this once backfill is supported
	return errors.New("not implemented")
}
