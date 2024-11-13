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

package deployment

import (
	"context"

	"go.temporal.io/server/common/namespace"
)

type (
	DeploymentActivities struct {
		activityDeps
		namespace   namespace.Name
		namespaceID namespace.ID
	}

	StartDeploymentNameWorkflowActivityInput struct {
		NamespaceName  string
		NamespaceID    string
		DeploymentName string
		// TODO Shivam - confirm if we need a buildID to be passed or not?
	}
)

// StartDeploymentNameWorkflow activity starts a DeploymentName workflow
func (a *DeploymentActivities) StartDeploymentNameWorkflow(ctx context.Context, input StartDeploymentNameWorkflowActivityInput) error {
	// TODO Shivam: Fill in the implementation to start DeploymentName Workflow
	return nil
}
