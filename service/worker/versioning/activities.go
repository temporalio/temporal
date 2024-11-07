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

package versioning

import (
	"context"

	"go.temporal.io/server/common/namespace"
)

type (
	activities struct {
		activityDeps
		namespace   namespace.Name
		namespaceID namespace.ID
	}
)

// Initialization of the activity happens in fx.go

// VerifyTaskQueueDefaultBuildID verifies if buildID is the default buildID for taskQueues in a deployment. Returns
// a list of all the task queues that have their default buildID as buildID.
func (a *activities) VerifyTaskQueueDefaultBuildID(ctx context.Context, deployment Deployment, buildID string) ([]*TaskQueue, error) {
	// TODO Shivam - pending implementation
	return nil, nil
}

// UpdateTaskQueueDefaultBuildID updates the default buildID for taskQueues in a deployment
func (a *activities) UpdateTaskQueueDefaultBuildID(ctx context.Context, deployment Deployment, buildID string) error {
	// TODO Shivam - pending implementation
	return nil
}
