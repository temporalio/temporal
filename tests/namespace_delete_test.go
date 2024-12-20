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

package tests

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/dgryski/go-farm"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type (
	namespaceTestSuite struct {
		*require.Assertions
		suite.Suite

		testClusterFactory testcore.TestClusterFactory

		frontendClient workflowservice.WorkflowServiceClient
		adminClient    adminservice.AdminServiceClient
		operatorClient operatorservice.OperatorServiceClient

		cluster       *testcore.TestCluster
		clusterConfig *testcore.TestClusterConfig
		logger        log.Logger
	}
)

func TestNamespaceSuite(t *testing.T) {
	// Test_NamespaceDelete_InvalidUTF8 can't run in parallel because of the global utf-8
	// validator. Enable this after the utf-8 stuff is gone.
	// TODO: enable this later: t.Parallel()
	suite.Run(t, &namespaceTestSuite{})
}

func (s *namespaceTestSuite) SetupSuite() {
	s.logger = log.NewTestLogger()
	s.testClusterFactory = testcore.NewTestClusterFactory()

	if testcore.UsingSQLAdvancedVisibility() {
		var err error
		s.clusterConfig, err = testcore.GetTestClusterConfig("testdata/cluster.yaml")
		s.Require().NoError(err)
		s.logger.Info(fmt.Sprintf("Running delete namespace tests with %s/%s persistence", testcore.TestFlags.PersistenceType, testcore.TestFlags.PersistenceDriver))
	} else {
		var err error
		// Elasticsearch is needed to test advanced visibility code path in reclaim resources workflow.
		s.clusterConfig, err = testcore.GetTestClusterConfig("testdata/es_cluster.yaml")
		s.Require().NoError(err)
		s.logger.Info("Running delete namespace tests with Elasticsearch persistence")
	}

	s.clusterConfig.DynamicConfigOverrides = map[dynamicconfig.Key]any{
		dynamicconfig.DeleteNamespaceDeleteActivityRPS.Key(): 1000,
	}

	cluster, err := s.testClusterFactory.NewCluster(s.T(), s.clusterConfig, s.logger)
	s.Require().NoError(err)
	s.cluster = cluster
	s.frontendClient = s.cluster.FrontendClient()
	s.adminClient = s.cluster.AdminClient()
	s.operatorClient = s.cluster.OperatorClient()
}

func (s *namespaceTestSuite) TearDownSuite() {
	_ = s.cluster.TearDownCluster()
}

func (s *namespaceTestSuite) SetupTest() {
	s.checkTestShard()

	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
}

func (s *namespaceTestSuite) Test_NamespaceDelete_InvalidUTF8() {
	// don't fail for this test, we're testing this behavior specifically
	s.cluster.OverrideDynamicConfig(s.T(), dynamicconfig.ValidateUTF8FailRPCRequest, false)
	s.cluster.OverrideDynamicConfig(s.T(), dynamicconfig.ValidateUTF8FailRPCResponse, false)
	s.cluster.OverrideDynamicConfig(s.T(), dynamicconfig.ValidateUTF8FailPersistence, false)

	capture := s.cluster.Host().CaptureMetricsHandler().StartCapture()
	defer s.cluster.Host().CaptureMetricsHandler().StopCapture(capture)

	ctx, cancel := rpc.NewContextWithTimeoutAndVersionHeaders(10000 * time.Second)
	defer cancel()
	s.False(utf8.Valid([]byte(testcore.InvalidUTF8)))
	retention := 24 * time.Hour
	_, err := s.frontendClient.RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        "valid-utf8", // we verify internally that these must be valid
		Description:                      testcore.InvalidUTF8,
		Data:                             map[string]string{testcore.InvalidUTF8: testcore.InvalidUTF8},
		WorkflowExecutionRetentionPeriod: durationpb.New(retention),
		HistoryArchivalState:             enumspb.ARCHIVAL_STATE_DISABLED,
		VisibilityArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
	})
	s.NoError(err)

	descResp, err := s.frontendClient.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: "valid-utf8",
	})
	s.NoError(err)
	nsID := descResp.GetNamespaceInfo().GetId()

	delResp, err := s.operatorClient.DeleteNamespace(ctx, &operatorservice.DeleteNamespaceRequest{
		NamespaceId: nsID,
	})
	s.NoError(err)
	s.Equal("valid-utf8-deleted-"+nsID[:5], delResp.GetDeletedNamespace())

	descResp2, err := s.frontendClient.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Id: nsID,
	})
	s.NoError(err)
	s.Equal(enumspb.NAMESPACE_STATE_DELETED, descResp2.GetNamespaceInfo().GetState())
	s.Eventually(func() bool {
		_, err := s.frontendClient.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
			Id: nsID,
		})
		var notFound *serviceerror.NamespaceNotFound
		if !errors.As(err, &notFound) {
			return false
		}

		return true
	}, 20*time.Second, time.Second)

	// check that we saw some validation errors
	s.NotEmpty(capture.Snapshot()[metrics.UTF8ValidationErrors.Name()])
}

func (s *namespaceTestSuite) Test_NamespaceDelete_Empty() {
	ctx, cancel := rpc.NewContextWithTimeoutAndVersionHeaders(10000 * time.Second)
	defer cancel()

	retention := 24 * time.Hour
	_, err := s.frontendClient.RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        "ns_name_san_diego",
		Description:                      "Namespace to delete",
		WorkflowExecutionRetentionPeriod: durationpb.New(retention),
		HistoryArchivalState:             enumspb.ARCHIVAL_STATE_DISABLED,
		VisibilityArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
	})
	s.NoError(err)

	descResp, err := s.frontendClient.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: "ns_name_san_diego",
	})
	s.NoError(err)
	nsID := descResp.GetNamespaceInfo().GetId()

	delResp, err := s.operatorClient.DeleteNamespace(ctx, &operatorservice.DeleteNamespaceRequest{
		Namespace: "ns_name_san_diego",
	})
	s.NoError(err)
	s.Equal("ns_name_san_diego-deleted-"+nsID[:5], delResp.GetDeletedNamespace())

	descResp2, err := s.frontendClient.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Id: nsID,
	})
	s.NoError(err)
	s.Equal(enumspb.NAMESPACE_STATE_DELETED, descResp2.GetNamespaceInfo().GetState())
	s.Eventually(func() bool {
		_, err := s.frontendClient.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
			Id: nsID,
		})
		var notFound *serviceerror.NamespaceNotFound
		if !errors.As(err, &notFound) {
			return false
		}

		return true
	}, 20*time.Second, time.Second)
}

func (s *namespaceTestSuite) Test_NamespaceDelete_OverrideDelay() {
	ctx, cancel := rpc.NewContextWithTimeoutAndVersionHeaders(10000 * time.Second)
	defer cancel()

	s.cluster.OverrideDynamicConfig(s.T(), dynamicconfig.DeleteNamespaceNamespaceDeleteDelay, time.Hour)

	retention := 24 * time.Hour
	_, err := s.frontendClient.RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        "ns_name_san_diego",
		Description:                      "Namespace to delete",
		WorkflowExecutionRetentionPeriod: durationpb.New(retention),
		HistoryArchivalState:             enumspb.ARCHIVAL_STATE_DISABLED,
		VisibilityArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
	})
	s.NoError(err)

	descResp, err := s.frontendClient.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: "ns_name_san_diego",
	})
	s.NoError(err)
	nsID := descResp.GetNamespaceInfo().GetId()

	delResp, err := s.operatorClient.DeleteNamespace(ctx, &operatorservice.DeleteNamespaceRequest{
		Namespace:            "ns_name_san_diego",
		NamespaceDeleteDelay: durationpb.New(0),
	})
	s.NoError(err)
	s.Equal("ns_name_san_diego-deleted-"+nsID[:5], delResp.GetDeletedNamespace())

	descResp2, err := s.frontendClient.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Id: nsID,
	})
	s.NoError(err)
	s.Equal(enumspb.NAMESPACE_STATE_DELETED, descResp2.GetNamespaceInfo().GetState())
	s.Eventually(func() bool {
		_, err := s.frontendClient.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
			Id: nsID,
		})
		var notFound *serviceerror.NamespaceNotFound
		if !errors.As(err, &notFound) {
			return false
		}

		return true
	}, 20*time.Second, time.Second)
}

func (s *namespaceTestSuite) Test_NamespaceDelete_Empty_WithID() {
	ctx, cancel := rpc.NewContextWithTimeoutAndVersionHeaders(10000 * time.Second)
	defer cancel()

	retention := 24 * time.Hour
	_, err := s.frontendClient.RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        "ns_name_san_diego",
		Description:                      "Namespace to delete",
		WorkflowExecutionRetentionPeriod: durationpb.New(retention),
		HistoryArchivalState:             enumspb.ARCHIVAL_STATE_DISABLED,
		VisibilityArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
	})
	s.NoError(err)

	descResp, err := s.frontendClient.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: "ns_name_san_diego",
	})
	s.NoError(err)
	nsID := descResp.GetNamespaceInfo().GetId()

	delResp, err := s.operatorClient.DeleteNamespace(ctx, &operatorservice.DeleteNamespaceRequest{
		NamespaceId: nsID,
	})
	s.NoError(err)
	s.Equal("ns_name_san_diego-deleted-"+nsID[:5], delResp.GetDeletedNamespace())

	descResp2, err := s.frontendClient.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Id: nsID,
	})
	s.NoError(err)
	s.Equal(enumspb.NAMESPACE_STATE_DELETED, descResp2.GetNamespaceInfo().GetState())
	s.Eventually(func() bool {
		_, err := s.frontendClient.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
			Id: nsID,
		})
		var notFound *serviceerror.NamespaceNotFound
		if !errors.As(err, &notFound) {
			return false
		}

		return true
	}, 20*time.Second, time.Second)
}

func (s *namespaceTestSuite) Test_NamespaceDelete_WithNameAndID() {
	ctx, cancel := rpc.NewContextWithTimeoutAndVersionHeaders(10000 * time.Second)
	defer cancel()

	retention := 24 * time.Hour
	_, err := s.frontendClient.RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        "ns_name_san_diego",
		Description:                      "Namespace to delete",
		WorkflowExecutionRetentionPeriod: durationpb.New(retention),
		HistoryArchivalState:             enumspb.ARCHIVAL_STATE_DISABLED,
		VisibilityArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
	})
	s.NoError(err)

	descResp, err := s.frontendClient.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: "ns_name_san_diego",
	})
	s.NoError(err)
	nsID := descResp.GetNamespaceInfo().GetId()

	_, err = s.operatorClient.DeleteNamespace(ctx, &operatorservice.DeleteNamespaceRequest{
		Namespace:   "ns_name_san_diego",
		NamespaceId: nsID,
	})
	s.EqualError(err, "Only one of namespace name or Id should be set on request.")
}

func (s *namespaceTestSuite) Test_NamespaceDelete_WithWorkflows() {
	ctx, cancel := rpc.NewContextWithTimeoutAndVersionHeaders(10000 * time.Second)
	defer cancel()

	retention := 24 * time.Hour
	_, err := s.frontendClient.RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        "ns_name_seattle",
		Description:                      "Namespace to delete",
		WorkflowExecutionRetentionPeriod: durationpb.New(retention),
		HistoryArchivalState:             enumspb.ARCHIVAL_STATE_DISABLED,
		VisibilityArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
	})
	s.NoError(err)

	descResp, err := s.frontendClient.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: "ns_name_seattle",
	})
	s.NoError(err)
	nsID := descResp.GetNamespaceInfo().GetId()

	// Start few workflow executions.
	var executions []*commonpb.WorkflowExecution
	for i := 0; i < 100; i++ {
		wid := "wf_id_" + strconv.Itoa(i)
		resp, err := s.frontendClient.StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
			RequestId:    uuid.New(),
			Namespace:    "ns_name_seattle",
			WorkflowId:   wid,
			WorkflowType: &commonpb.WorkflowType{Name: "workflowTypeName"},
			TaskQueue:    &taskqueuepb.TaskQueue{Name: "taskQueueName", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		s.NoError(err)
		executions = append(executions, &commonpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      resp.GetRunId(),
		})
	}

	// Terminate some workflow executions.
	for _, execution := range executions[:30] {
		_, err = s.frontendClient.TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
			Namespace:         "ns_name_seattle",
			WorkflowExecution: execution,
		})
		s.NoError(err)
	}

	delResp, err := s.operatorClient.DeleteNamespace(ctx, &operatorservice.DeleteNamespaceRequest{
		Namespace: "ns_name_seattle",
	})
	s.NoError(err)
	s.Equal("ns_name_seattle-deleted-"+nsID[:5], delResp.GetDeletedNamespace())

	descResp2, err := s.frontendClient.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Id: nsID,
	})
	s.NoError(err)
	s.Equal(enumspb.NAMESPACE_STATE_DELETED, descResp2.GetNamespaceInfo().GetState())

	s.Eventually(func() bool {
		_, err := s.frontendClient.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
			Id: nsID,
		})
		var notFound *serviceerror.NamespaceNotFound
		if !errors.As(err, &notFound) {
			return false // namespace still exists
		}

		for _, execution := range executions {
			_, err = s.frontendClient.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: "ns_name_seattle",
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: execution.GetWorkflowId(),
				},
			})
			if !errors.As(err, &notFound) {
				return false // should never happen
			}
		}
		return true
	}, 20*time.Second, time.Second)
}

func (s *namespaceTestSuite) Test_NamespaceDelete_WithMissingWorkflows() {
	ctx, cancel := rpc.NewContextWithTimeoutAndVersionHeaders(10000 * time.Second)
	defer cancel()

	retention := 24 * time.Hour
	_, err := s.frontendClient.RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        "ns_name_los_angeles",
		Description:                      "Namespace to delete",
		WorkflowExecutionRetentionPeriod: durationpb.New(retention),
		HistoryArchivalState:             enumspb.ARCHIVAL_STATE_DISABLED,
		VisibilityArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
	})
	s.NoError(err)

	descResp, err := s.frontendClient.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: "ns_name_los_angeles",
	})
	s.NoError(err)
	nsID := descResp.GetNamespaceInfo().GetId()

	// Start few workflow executions.

	var executions []*commonpb.WorkflowExecution
	for i := 0; i < 10; i++ {
		wid := "wf_id_" + strconv.Itoa(i)
		resp, err := s.frontendClient.StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
			RequestId:    uuid.New(),
			Namespace:    "ns_name_los_angeles",
			WorkflowId:   wid,
			WorkflowType: &commonpb.WorkflowType{Name: "workflowTypeName"},
			TaskQueue:    &taskqueuepb.TaskQueue{Name: "taskQueueName", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		s.NoError(err)
		executions = append(executions, &commonpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      resp.GetRunId(),
		})
	}

	// Delete some workflow executions from DB but not from visibility.
	// Every subsequent delete (from deleteexecutions.Workflow) from ES will take at least 1s due to bulk processor.
	for _, execution := range executions[0:5] {
		shardID := common.WorkflowIDToHistoryShard(
			nsID,
			execution.GetWorkflowId(),
			s.clusterConfig.HistoryConfig.NumHistoryShards,
		)

		err = s.cluster.ExecutionManager().DeleteWorkflowExecution(ctx, &persistence.DeleteWorkflowExecutionRequest{
			ShardID:     shardID,
			NamespaceID: nsID,
			WorkflowID:  execution.GetWorkflowId(),
			RunID:       execution.GetRunId(),
		})
		s.NoError(err)
	}

	delResp, err := s.operatorClient.DeleteNamespace(ctx, &operatorservice.DeleteNamespaceRequest{
		Namespace: "ns_name_los_angeles",
	})
	s.NoError(err)
	s.Equal("ns_name_los_angeles-deleted-"+nsID[:5], delResp.GetDeletedNamespace())

	descResp2, err := s.frontendClient.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Id: nsID,
	})
	s.NoError(err)
	s.Equal(enumspb.NAMESPACE_STATE_DELETED, descResp2.GetNamespaceInfo().GetState())

	s.Eventually(func() bool {
		_, err := s.frontendClient.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
			Id: nsID,
		})
		var notFound *serviceerror.NamespaceNotFound
		if !errors.As(err, &notFound) {
			return false // namespace still exists
		}

		for _, execution := range executions {
			_, err = s.frontendClient.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: "ns_name_los_angeles",
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: execution.GetWorkflowId(),
				},
			})
			if !errors.As(err, &notFound) {
				return false // should never happen
			}
		}
		return true
	}, 20*time.Second, time.Second)
}

func (s *namespaceTestSuite) Test_NamespaceDelete_CrossNamespaceChild() {
	// TODO (alex): create 2 namespaces, start workflow in first namespace and start child in second namespace.
	// Delete second namespace and verify that parent received child termination signal.
}

// checkTestShard supports test sharding based on environment variables.
func (s *namespaceTestSuite) checkTestShard() {
	totalStr := os.Getenv("TEST_TOTAL_SHARDS")
	indexStr := os.Getenv("TEST_SHARD_INDEX")
	if totalStr == "" || indexStr == "" {
		return
	}
	total, err := strconv.Atoi(totalStr)
	if err != nil || total < 1 {
		s.T().Fatal("Couldn't convert TEST_TOTAL_SHARDS")
	}
	index, err := strconv.Atoi(indexStr)
	if err != nil || index < 0 || index >= total {
		s.T().Fatal("Couldn't convert TEST_SHARD_INDEX")
	}

	// This was determined empirically to distribute our existing test names
	// reasonably well. This can be adjusted from time to time.
	// For parallelism 4, use 11. For 3, use 26. For 2, use 20.
	const salt = "-salt-26"

	nameToHash := s.T().Name() + salt
	testIndex := int(farm.Fingerprint32([]byte(nameToHash))) % total
	if testIndex != index {
		s.T().Skipf("Skipping %s in test shard %d/%d (it runs in %d)", s.T().Name(), index+1, total, testIndex+1)
	}
	s.T().Logf("Running %s in test shard %d/%d", s.T().Name(), index+1, total)
}

func (s *namespaceTestSuite) Test_NamespaceDelete_Protected() {
	ctx := context.Background()

	tv := testvars.New(s.T())

	_, err := s.frontendClient.RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        tv.NamespaceName().String(),
		Description:                      tv.Any().String(),
		WorkflowExecutionRetentionPeriod: tv.InfiniteTimeout(),
		HistoryArchivalState:             enumspb.ARCHIVAL_STATE_DISABLED,
		VisibilityArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
	})
	s.NoError(err)

	s.cluster.OverrideDynamicConfig(s.T(), dynamicconfig.ProtectedNamespaces, []string{tv.NamespaceName().String()})

	delResp, err := s.operatorClient.DeleteNamespace(ctx, &operatorservice.DeleteNamespaceRequest{
		Namespace: tv.NamespaceName().String(),
	})
	s.Error(err)
	s.Nil(delResp)

	var invalidArgErr *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArgErr)
	s.Equal(fmt.Sprintf("namespace %s is protected from deletion", tv.NamespaceName().String()), invalidArgErr.Message)
}
