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

package testcore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/testing/historyrequire"
	"go.temporal.io/server/components/callbacks"
	"go.temporal.io/server/components/nexusoperations"
)

type (
	ClientFunctionalSuite struct {
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
		FunctionalTestBase
		historyrequire.HistoryRequire
		sdkClient sdkclient.Client
		worker    worker.Worker
		taskQueue string

		baseConfigPath string
	}
)

var (
	ErrEncodingIsNotSet       = errors.New("payload encoding metadata is not set")
	ErrEncodingIsNotSupported = errors.New("payload encoding is not supported")
)

func (s *ClientFunctionalSuite) Worker() worker.Worker {
	return s.worker
}

func (s *ClientFunctionalSuite) SdkClient() sdkclient.Client {
	return s.sdkClient
}

func (s *ClientFunctionalSuite) TaskQueue() string {
	return s.taskQueue
}

func (s *ClientFunctionalSuite) SetupSuite() {
	// these limits are higher in production, but our tests would take too long if we set them that high
	dynamicConfigOverrides := map[dynamicconfig.Key]any{
		dynamicconfig.NumPendingChildExecutionsLimitError.Key():             ClientSuiteLimit,
		dynamicconfig.NumPendingActivitiesLimitError.Key():                  ClientSuiteLimit,
		dynamicconfig.NumPendingCancelRequestsLimitError.Key():              ClientSuiteLimit,
		dynamicconfig.NumPendingSignalsLimitError.Key():                     ClientSuiteLimit,
		dynamicconfig.FrontendEnableWorkerVersioningDataAPIs.Key():          true,
		dynamicconfig.FrontendEnableWorkerVersioningWorkflowAPIs.Key():      true,
		dynamicconfig.FrontendMaxConcurrentBatchOperationPerNamespace.Key(): ClientSuiteLimit,
		dynamicconfig.EnableNexus.Key():                                     true,
		dynamicconfig.RefreshNexusEndpointsMinWait.Key():                    1 * time.Millisecond,
		callbacks.AllowedAddresses.Key():                                    []any{map[string]any{"Pattern": "*", "AllowInsecure": true}},
	}
	s.SetDynamicConfigOverrides(dynamicConfigOverrides)
	s.FunctionalTestBase.SetupSuite("testdata/client_cluster.yaml")

}

func (s *ClientFunctionalSuite) TearDownSuite() {
	s.FunctionalTestBase.TearDownSuite()
}

func (s *ClientFunctionalSuite) SetupTest() {
	s.FunctionalTestBase.SetupTest()

	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
	s.HistoryRequire = historyrequire.New(s.T())

	// Set URL template after httpAPAddress is set, see commonnexus.RouteCompletionCallback
	s.OverrideDynamicConfig(
		nexusoperations.CallbackURLTemplate,
		"http://"+s.HttpAPIAddress()+"/namespaces/{{.NamespaceName}}/nexus/callback")

	sdkClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.FrontendGRPCAddress(),
		Namespace: s.Namespace(),
	})
	if err != nil {
		s.Logger.Fatal("Error when creating SDK client", tag.Error(err))
	}
	s.sdkClient = sdkClient
	s.taskQueue = RandomizeStr("tq")

	// We need to set this timeout to 0 to disable the deadlock detector. Otherwise, the deadlock detector will cause
	// TestTooManyChildWorkflows to fail because it thinks there is a deadlock due to the blocked child workflows.
	s.worker = worker.New(s.sdkClient, s.taskQueue, worker.Options{DeadlockDetectionTimeout: 0})
	if err := s.worker.Start(); err != nil {
		s.Logger.Fatal("Error when start worker", tag.Error(err))
	}
}

func (s *ClientFunctionalSuite) TearDownTest() {
	if s.worker != nil {
		s.worker.Stop()
	}
	if s.sdkClient != nil {
		s.sdkClient.Close()
	}
}

func (s *ClientFunctionalSuite) EventuallySucceeds(ctx context.Context, operationCtx backoff.OperationCtx) {
	s.T().Helper()
	s.NoError(backoff.ThrottleRetryContext(
		ctx,
		operationCtx,
		backoff.NewExponentialRetryPolicy(time.Second),
		func(err error) bool {
			// all errors are retryable
			return true
		},
	))
}

func (s *ClientFunctionalSuite) HistoryContainsFailureCausedBy(
	ctx context.Context,
	workflowId string,
	cause enumspb.WorkflowTaskFailedCause,
) {
	s.T().Helper()
	s.EventuallySucceeds(ctx, func(ctx context.Context) error {
		history := s.sdkClient.GetWorkflowHistory(
			ctx,
			workflowId,
			"",
			true,
			enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT,
		)
		for history.HasNext() {
			event, err := history.Next()
			s.NoError(err)
			switch a := event.Attributes.(type) {
			case *historypb.HistoryEvent_WorkflowTaskFailedEventAttributes:
				if a.WorkflowTaskFailedEventAttributes.Cause == cause {
					return nil
				}
			}
		}
		return fmt.Errorf("did not find a failed task whose cause was %q", cause)
	})
}

// Uncomment if you need to debug history.
// func (s *ClientFunctionalSuite) printHistory(workflowID string, runID string) {
// 	iter := s.sdkClient.GetWorkflowHistory(context.Background(), workflowID, runID, false, 0)
// 	history := &historypb.History{}
// 	for iter.HasNext() {
// 		event, err := iter.Next()
// 		s.NoError(err)
// 		history.Events = append(history.Events, event)
// 	}
// 	common.PrettyPrintHistory(history, s.Logger)
// }
