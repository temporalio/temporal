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

package cli

import (
	"github.com/uber/cadence/.gen/go/admin/adminserviceclient"
	s "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/urfave/cli"
)

func getAdminServiceClient(c *cli.Context) adminserviceclient.Interface {
	client, err := cBuilder.BuildAdminServiceClient(c)
	if err != nil {
		ExitIfError(err)
	}

	return client
}

// InquiryWorkflow inquiries a new workflow execution
func InquiryWorkflow(c *cli.Context) {
	// using service client instead of cadence.Client because we need to directly pass the json blob as input.
	serviceClient := getAdminServiceClient(c)

	domain := getRequiredGlobalOption(c, FlagDomain)
	wid := getRequiredOption(c, FlagWorkflowID)
	rid := c.String(FlagRunID)

	ctx, cancel := newContext()
	defer cancel()

	resp, err := serviceClient.InquiryWorkflowExecution(ctx, &s.DescribeWorkflowExecutionRequest{
		Domain: common.StringPtr(domain),
		Execution: &s.WorkflowExecution{
			WorkflowId: common.StringPtr(wid),
			RunId:      common.StringPtr(rid),
		},
	})
	if err != nil {
		ErrorAndExit("Inquiry workflow execution failed", err)
	}

	prettyPrintJSONObject(resp)
}
