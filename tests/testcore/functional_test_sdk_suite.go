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

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/components/callbacks"
	"go.temporal.io/server/components/nexusoperations"
)

type (
	// TODO (alex): merge this with FunctionalTestBase.
	FunctionalTestSdkSuite struct {
		FunctionalTestBase

		// Suites can override client or worker options by modifying these before calling
		// FunctionalTestSdkSuite.SetupSuite. ClientOptions.HostPort and Namespace cannot be
		// overridden.
		ClientOptions sdkclient.Options
		WorkerOptions worker.Options

		sdkClient sdkclient.Client
		worker    worker.Worker
		taskQueue string
	}
)

// TODO (alex): move this to test_data_converter.go where it is actually used.
var (
	ErrEncodingIsNotSet       = errors.New("payload encoding metadata is not set")
	ErrEncodingIsNotSupported = errors.New("payload encoding is not supported")
)

func (s *FunctionalTestSdkSuite) Worker() worker.Worker {
	return s.worker
}

func (s *FunctionalTestSdkSuite) SdkClient() sdkclient.Client {
	return s.sdkClient
}

func (s *FunctionalTestSdkSuite) TaskQueue() string {
	return s.taskQueue
}

func (s *FunctionalTestSdkSuite) SetupSuite() {
	// these limits are higher in production, but our tests would take too long if we set them that high
	dynamicConfigOverrides := map[dynamicconfig.Key]any{
		dynamicconfig.NumPendingChildExecutionsLimitError.Key():             ClientSuiteLimit,
		dynamicconfig.NumPendingActivitiesLimitError.Key():                  ClientSuiteLimit,
		dynamicconfig.NumPendingCancelRequestsLimitError.Key():              ClientSuiteLimit,
		dynamicconfig.NumPendingSignalsLimitError.Key():                     ClientSuiteLimit,
		dynamicconfig.FrontendEnableWorkerVersioningDataAPIs.Key():          true,
		dynamicconfig.FrontendEnableWorkerVersioningWorkflowAPIs.Key():      true,
		dynamicconfig.FrontendMaxConcurrentBatchOperationPerNamespace.Key(): ClientSuiteLimit,
		dynamicconfig.RefreshNexusEndpointsMinWait.Key():                    1 * time.Millisecond,
		callbacks.AllowedAddresses.Key():                                    []any{map[string]any{"Pattern": "*", "AllowInsecure": true}},
	}

	s.FunctionalTestBase.SetupSuiteWithCluster("testdata/es_cluster.yaml", WithDynamicConfigOverrides(dynamicConfigOverrides))
}

func (s *FunctionalTestSdkSuite) SetupTest() {
	s.FunctionalTestBase.SetupTest()

	// Set URL template after httpAPAddress is set, see commonnexus.RouteCompletionCallback
	s.OverrideDynamicConfig(
		nexusoperations.CallbackURLTemplate,
		"http://"+s.HttpAPIAddress()+"/namespaces/{{.NamespaceName}}/nexus/callback")

	s.ClientOptions.HostPort = s.FrontendGRPCAddress()
	s.ClientOptions.Namespace = s.Namespace().String()
	if s.ClientOptions.Logger == nil {
		s.ClientOptions.Logger = log.NewSdkLogger(s.Logger)
	}

	var err error
	s.sdkClient, err = sdkclient.Dial(s.ClientOptions)
	s.NoError(err)
	s.taskQueue = RandomizeStr("tq")

	s.worker = worker.New(s.sdkClient, s.taskQueue, s.WorkerOptions)
	err = s.worker.Start()
	s.NoError(err)
}

func (s *FunctionalTestSdkSuite) TearDownTest() {
	if s.worker != nil {
		s.worker.Stop()
	}
	if s.sdkClient != nil {
		s.sdkClient.Close()
	}
	s.FunctionalTestBase.TearDownTest()
}

func (s *FunctionalTestSdkSuite) EventuallySucceeds(ctx context.Context, operationCtx backoff.OperationCtx) {
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

func (s *FunctionalTestSdkSuite) HistoryContainsFailureCausedBy(
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
