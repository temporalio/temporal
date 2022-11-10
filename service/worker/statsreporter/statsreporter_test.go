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

package statsreporter

import (
	"context"
	"testing"

	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/quotas"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

type statsReporterTestSuite struct {
	suite.Suite
	controller    *gomock.Controller
	logger        log.Logger
	metricHandler metrics.MetricsHandler

	metadataManager       *persistence.MockMetadataManager
	visibilityManager     *manager.MockVisibilityManager
	serviceResolver       *membership.MockServiceResolver
	visibilityRateLimiter *quotas.MockRateLimiter
}

func (s *statsReporterTestSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.metadataManager = persistence.NewMockMetadataManager(s.controller)
	s.visibilityManager = manager.NewMockVisibilityManager(s.controller)
	s.serviceResolver = membership.NewMockServiceResolver(s.controller)
	s.visibilityRateLimiter = quotas.NewMockRateLimiter(s.controller)
}

func TestStatsReporter(t *testing.T) {
	suite.Run(t, new(statsReporterTestSuite))
}

func (s *statsReporterTestSuite) TestReportStats() {
	s.visibilityRateLimiter.EXPECT().Wait(gomock.Any()).Times(1)
	ns1 := &persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:    namespace.NewID().String(),
				Name:  "ns1",
				State: enumspb.NAMESPACE_STATE_REGISTERED,
			},
		},
	}
	ns2 := &persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:    namespace.NewID().String(),
				Name:  "ns2",
				State: enumspb.NAMESPACE_STATE_REGISTERED,
			},
		},
	}
	s.metadataManager.EXPECT().ListNamespaces(gomock.Any(), &persistence.ListNamespacesRequest{
		PageSize:      listNamespacePageSize,
		NextPageToken: nil,
	}).Return(&persistence.ListNamespacesResponse{
		Namespaces: []*persistence.GetNamespaceResponse{ns1, ns2},
	}, nil)

	s.visibilityManager.EXPECT().CountWorkflowExecutions(gomock.Any(), &manager.CountWorkflowExecutionsRequest{
		NamespaceID: namespace.ID(ns1.Namespace.Info.Id),
		Namespace:   namespace.Name(ns1.Namespace.Info.Name),
		Query:       "ExecutionStatus='Running'",
	}).Return(&manager.CountWorkflowExecutionsResponse{Count: 5}, nil)

	statsReporter := &StatsReporter{
		Logger:            log.NewTestLogger(),
		MetricsHandler:    metrics.NoopMetricsHandler,
		MetadataManager:   s.metadataManager,
		VisibilityManager: s.visibilityManager,
		EmitOpenWorkflowCount: func(namespace string) bool {
			if namespace == ns1.Namespace.Info.Name {
				return true
			}
			return false
		},
		VisibilityRateLimiter: s.visibilityRateLimiter,
	}
	statsReporter.reportNamespaceStats(context.Background())
}
