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

package migration

import (
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
)

func TestHandoverWorkflow(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	namespaceID := uuid.New()

	var a *activities
	env.OnActivity(a.GetMetadata, mock.Anything, metadataRequest{Namespace: "test-ns"}).Return(&metadataResponse{ShardCount: 4, NamespaceID: namespaceID}, nil)

	env.OnActivity(a.GetMaxReplicationTaskIDs, mock.Anything).Return(
		&replicationStatus{map[int32]int64{
			1: 100,
			2: 100,
			3: 100,
			4: 100,
		},
		},
		nil,
	)

	env.OnActivity(a.WaitReplication, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(a.UpdateNamespaceState, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(a.WaitHandover, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(a.UpdateActiveCluster, mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(NamespaceHandoverWorkflow, NamespaceHandoverParams{
		Namespace:              "test-ns",
		RemoteCluster:          "test-remote",
		AllowedLaggingSeconds:  10,
		HandoverTimeoutSeconds: 30,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}
