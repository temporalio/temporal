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
	"time"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence/visibility"
)

var (
	// Functional tests don't use dynamic config files. All settings get their default values
	// defined in common/dynamicconfig/constants.go.
	//
	// There are 4 ways to override a setting:
	// 1. Globally using this file. Every test suite creates a new test cluster using this overrides.
	// 2. Per test suite using FunctionalTestBase.SetupSuiteWithCluster() and WithDynamicConfigOverrides() option.
	// 3. Per test using FunctionalTestBase.OverrideDynamicConfig() method.
	// 4. Per specific cluster per test (if test has more than one cluster) using TestCluster.OverrideDynamicConfig() method.
	//
	// NOTE1: settings which are not really dynamic (requires server restart to take effect) can't be overridden on test level,
	//        i.e., must be overridden globally (1) or per test suite (2).
	// NOTE2: per test overrides change the value for the cluster, therefore, it affects not only a specific test, but
	//        all tests for that suite. The automatic cleanup reverts to the  previous value and tests don't affect each other.
	//        But that means tests in the same suite can't be run in parallel. This is not a problem because testify
	//        doesn't allow parallel execution of tests in the same suite anyway. If one day, it will be allowed,
	//        unique namespaces with overrides per namespace should be used for tests that require overrides.
	dynamicConfigOverrides = map[dynamicconfig.Key]any{
		dynamicconfig.FrontendRPS.Key():                                         3000,
		dynamicconfig.FrontendMaxNamespaceVisibilityRPSPerInstance.Key():        50,
		dynamicconfig.FrontendMaxNamespaceVisibilityBurstRatioPerInstance.Key(): 1,
		dynamicconfig.ReplicationTaskProcessorErrorRetryMaxAttempts.Key():       1,
		dynamicconfig.SecondaryVisibilityWritingMode.Key():                      visibility.SecondaryVisibilityWritingModeOff,
		dynamicconfig.WorkflowTaskHeartbeatTimeout.Key():                        5 * time.Second,
		dynamicconfig.ReplicationTaskFetcherAggregationInterval.Key():           200 * time.Millisecond,
		dynamicconfig.ReplicationTaskFetcherErrorRetryWait.Key():                50 * time.Millisecond,
		dynamicconfig.ReplicationTaskProcessorErrorRetryWait.Key():              time.Millisecond,
		dynamicconfig.ClusterMetadataRefreshInterval.Key():                      100 * time.Millisecond,
		dynamicconfig.NamespaceCacheRefreshInterval.Key():                       NamespaceCacheRefreshInterval,
		dynamicconfig.ReplicationEnableUpdateWithNewTaskMerge.Key():             true,
		dynamicconfig.ValidateUTF8SampleRPCRequest.Key():                        1.0,
		dynamicconfig.ValidateUTF8SampleRPCResponse.Key():                       1.0,
		dynamicconfig.ValidateUTF8SamplePersistence.Key():                       1.0,
		dynamicconfig.ValidateUTF8FailRPCRequest.Key():                          true,
		dynamicconfig.ValidateUTF8FailRPCResponse.Key():                         true,
		dynamicconfig.ValidateUTF8FailPersistence.Key():                         true,
		dynamicconfig.EnableWorkflowExecutionTimeoutTimer.Key():                 true,
		dynamicconfig.FrontendMaskInternalErrorDetails.Key():                    false,
		dynamicconfig.HistoryScannerEnabled.Key():                               false,
		dynamicconfig.TaskQueueScannerEnabled.Key():                             false,
		dynamicconfig.ExecutionsScannerEnabled.Key():                            false,
		dynamicconfig.BuildIdScavengerEnabled.Key():                             false,

		// Better to read through in tests than add artificial sleeps (which is what we previously had).
		dynamicconfig.ForceSearchAttributesCacheRefreshOnRead.Key(): true,

		dynamicconfig.RetentionTimerJitterDuration.Key():        time.Second,
		dynamicconfig.EnableEagerWorkflowStart.Key():            true,
		dynamicconfig.FrontendEnableExecuteMultiOperation.Key(): true,
		dynamicconfig.ActivityAPIsEnabled.Key():                 true,
	}
)
