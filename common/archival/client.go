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

package archival

import (
	"context"
	"errors"
	"github.com/uber/cadence/.gen/go/shared"
	"time"
)

// PutRequest is request for Archive
type PutRequest struct {
	DomainName   string
	DomainID     string
	WorkflowID   string
	RunID        string
	WorkflowType string
	Status       string
	CloseTime    time.Time
	History      *shared.History
}

// Client is used to store and retrieve blobs
type Client interface {
	PutWorkflow(ctx context.Context, request *PutRequest) error

	GetWorkflowExecutionHistory(
		ctx context.Context,
		request *shared.GetWorkflowExecutionHistoryRequest,
	) (*shared.GetWorkflowExecutionHistoryResponse, error)

	ListClosedWorkflowExecutions(
		ctx context.Context,
		ListRequest *shared.ListClosedWorkflowExecutionsRequest,
	) (*shared.ListClosedWorkflowExecutionsResponse, error)
}

type nopClient struct{}

func (c *nopClient) PutWorkflow(ctx context.Context, request *PutRequest) error {
	return errors.New("not implemented")
}

func (c *nopClient) GetWorkflowExecutionHistory(
	ctx context.Context,
	request *shared.GetWorkflowExecutionHistoryRequest,
) (*shared.GetWorkflowExecutionHistoryResponse, error) {
	return nil, errors.New("not implemented")
}

func (c *nopClient) ListClosedWorkflowExecutions(
	ctx context.Context,
	ListRequest *shared.ListClosedWorkflowExecutionsRequest,
) (*shared.ListClosedWorkflowExecutionsResponse, error) {
	return nil, errors.New("not implemented")
}

// NewNopClient creates a nop client
func NewNopClient() Client {
	return &nopClient{}
}
