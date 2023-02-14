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
	"errors"
	"flag"
	"fmt"
	"strconv"
	"testing"
	"time"

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
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
	esclient "go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/rpc"
)

type (
	namespaceTestSuite struct {
		*require.Assertions
		suite.Suite

		frontendClient workflowservice.WorkflowServiceClient
		adminClient    adminservice.AdminServiceClient
		operatorClient operatorservice.OperatorServiceClient
		esClient       esclient.IntegrationTestsClient

		cluster       *TestCluster
		clusterConfig *TestClusterConfig
		logger        log.Logger
	}
)

func TestNamespaceSuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, &namespaceTestSuite{})
}

func dynamicConfig() map[dynamicconfig.Key]interface{} {
	return map[dynamicconfig.Key]interface{}{
		dynamicconfig.DeleteNamespaceDeleteActivityRPS: 1000,
	}
}

func (s *namespaceTestSuite) SetupSuite() {
	s.logger = log.NewTestLogger()

	switch TestFlags.PersistenceDriver {
	case mysql.PluginNameV8, postgresql.PluginNameV12, sqlite.PluginName:
		var err error
		s.clusterConfig, err = GetTestClusterConfig("testdata/integration_test_cluster.yaml")
		s.Require().NoError(err)
		s.logger.Info(fmt.Sprintf("Running delete namespace tests with %s/%s persistence", TestFlags.PersistenceType, TestFlags.PersistenceDriver))
	default:
		var err error
		s.clusterConfig, err = GetTestClusterConfig("testdata/integration_test_es_cluster.yaml")
		s.Require().NoError(err)
		s.logger.Info("Running delete namespace tests with Elasticsearch persistence")
		// Elasticsearch is needed to test advanced visibility code path in reclaim resources workflow.
		s.esClient = CreateESClient(&s.Suite, s.clusterConfig.ESConfig, s.logger)
		CreateIndex(&s.Suite, s.esClient, "../schema/elasticsearch/visibility/index_template_v7.json", s.clusterConfig.ESConfig.GetVisibilityIndex())
	}

	s.clusterConfig.DynamicConfigOverrides = dynamicConfig()

	cluster, err := NewCluster(s.clusterConfig, s.logger)
	s.Require().NoError(err)
	s.cluster = cluster
	s.frontendClient = s.cluster.GetFrontendClient()
	s.adminClient = s.cluster.GetAdminClient()
	s.operatorClient = s.cluster.GetOperatorClient()
}

func (s *namespaceTestSuite) TearDownSuite() {
	s.cluster.TearDownCluster()
	if s.esClient != nil {
		DeleteIndex(&s.Suite, s.esClient, s.clusterConfig.ESConfig.GetVisibilityIndex())
	}
}

func (s *namespaceTestSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
}

func (s *namespaceTestSuite) Test_NamespaceDelete_Empty() {
	ctx, cancel := rpc.NewContextWithTimeoutAndVersionHeaders(10000 * time.Second)
	defer cancel()

	retention := 24 * time.Hour
	_, err := s.frontendClient.RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        "ns_name_san_diego",
		Description:                      "Namespace to delete",
		WorkflowExecutionRetentionPeriod: &retention,
		HistoryArchivalState:             enumspb.ARCHIVAL_STATE_DISABLED,
		VisibilityArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
	})
	s.NoError(err)
	// DescribeNamespace reads directly from database but namespace validator uses cache.
	s.cluster.RefreshNamespaceCache()

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

	namespaceExistsOp := func() error {
		_, err := s.frontendClient.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
			Id: nsID,
		})
		var notFound *serviceerror.NamespaceNotFound
		if errors.As(err, &notFound) {
			return nil
		}
		return errors.New("namespace still exists")
	}

	namespaceExistsPolicy := backoff.NewExponentialRetryPolicy(time.Second).
		WithBackoffCoefficient(1).
		WithExpirationInterval(30 * time.Second)

	err = backoff.ThrottleRetry(namespaceExistsOp, namespaceExistsPolicy, func(_ error) bool { return true })
	s.NoError(err)
}

func (s *namespaceTestSuite) Test_NamespaceDelete_WithWorkflows() {
	ctx, cancel := rpc.NewContextWithTimeoutAndVersionHeaders(10000 * time.Second)
	defer cancel()

	retention := 24 * time.Hour
	_, err := s.frontendClient.RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        "ns_name_los_angeles",
		Description:                      "Namespace to delete",
		WorkflowExecutionRetentionPeriod: &retention,
		HistoryArchivalState:             enumspb.ARCHIVAL_STATE_DISABLED,
		VisibilityArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
	})
	s.NoError(err)
	// DescribeNamespace reads directly from database but namespace validator uses cache.
	s.cluster.RefreshNamespaceCache()

	descResp, err := s.frontendClient.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: "ns_name_los_angeles",
	})
	s.NoError(err)
	nsID := descResp.GetNamespaceInfo().GetId()

	// Start few workflow executions.
	for i := 0; i < 100; i++ {
		_, err = s.frontendClient.StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
			RequestId:    uuid.New(),
			Namespace:    "ns_name_los_angeles",
			WorkflowId:   "wf_id_" + strconv.Itoa(i),
			WorkflowType: &commonpb.WorkflowType{Name: "workflowTypeName"},
			TaskQueue:    &taskqueuepb.TaskQueue{Name: "taskQueueName"},
		})
		s.NoError(err)
	}

	// Terminate some workflow executions.
	for i := 0; i < 30; i++ {
		_, err = s.frontendClient.TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
			Namespace: "ns_name_los_angeles",
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: "wf_id_" + strconv.Itoa(i),
			},
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

	namespaceExistsOp := func() error {
		_, err := s.frontendClient.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
			Id: nsID,
		})
		var notFound *serviceerror.NamespaceNotFound
		if errors.As(err, &notFound) {
			_, err0 := s.frontendClient.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: "ns_name_los_angeles",
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "wf_id_0",
				},
			})
			_, err99 := s.frontendClient.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: "ns_name_los_angeles",
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "wf_id_99",
				},
			})
			if errors.As(err0, &notFound) && errors.As(err99, &notFound) {
				return nil
			}
		}
		return errors.New("namespace still exists")
	}

	namespaceExistsPolicy := backoff.NewExponentialRetryPolicy(time.Second).
		WithBackoffCoefficient(1).
		WithExpirationInterval(30 * time.Second)

	err = backoff.ThrottleRetry(namespaceExistsOp, namespaceExistsPolicy, func(_ error) bool { return true })
	s.NoError(err)
}

func (s *namespaceTestSuite) Test_NamespaceDelete_CrossNamespaceChild() {
	// TODO (alex): create 2 namespaces, start workflow in first namespace and start child in second namespace.
	// Delete second namespace and verify that parent received child termination signal.
}
