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

package host

import (
	"go.uber.org/yarpc"

	"github.com/temporalio/temporal-proto/workflowservice"
	"github.com/temporalio/temporal/.gen/go/admin/adminserviceclient"
	"github.com/temporalio/temporal/.gen/go/history/historyserviceclient"
	"github.com/temporalio/temporal/common"
)

// AdminClient is the interface exposed by admin service client
type AdminClient interface {
	adminserviceclient.Interface
}

// FrontendClient is the interface exposed by frontend service client
type FrontendClient interface {
	workflowservice.WorkflowServiceYARPCClient
}

// HistoryClient is the interface exposed by history service client
type HistoryClient interface {
	historyserviceclient.Interface
}

// NewAdminClient creates a client to cadence admin client
func NewAdminClient(d *yarpc.Dispatcher) AdminClient {
	return adminserviceclient.New(d.ClientConfig(common.FrontendServiceName))
}

// NewFrontendClient creates a client to cadence frontend client
func NewFrontendClient(d *yarpc.Dispatcher) workflowservice.WorkflowServiceYARPCClient {
	return workflowservice.NewWorkflowServiceYARPCClient(d.ClientConfig(common.FrontendServiceName))
}

// NewHistoryClient creates a client to cadence history service client
func NewHistoryClient(d *yarpc.Dispatcher) HistoryClient {
	return historyserviceclient.New(d.ClientConfig(common.HistoryServiceName))
}
