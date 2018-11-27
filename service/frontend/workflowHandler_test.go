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

package frontend

import (
	"context"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/uber-common/bark"
	"github.com/uber-go/tally"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	cs "github.com/uber/cadence/common/service"
	dc "github.com/uber/cadence/common/service/dynamicconfig"
)

func TestMergeDomainData_Overriding(t *testing.T) {
	wh := &WorkflowHandler{}
	out := wh.mergeDomainData(
		map[string]string{
			"k0": "v0",
		},
		map[string]string{
			"k0": "v2",
		},
	)

	assert.Equal(t, map[string]string{
		"k0": "v2",
	}, out)
}

func TestMergeDomainData_Adding(t *testing.T) {
	wh := &WorkflowHandler{}
	out := wh.mergeDomainData(
		map[string]string{
			"k0": "v0",
		},
		map[string]string{
			"k1": "v2",
		},
	)

	assert.Equal(t, map[string]string{
		"k0": "v0",
		"k1": "v2",
	}, out)
}

func TestMergeDomainData_Merging(t *testing.T) {
	wh := &WorkflowHandler{}
	out := wh.mergeDomainData(
		map[string]string{
			"k0": "v0",
		},
		map[string]string{
			"k0": "v1",
			"k1": "v2",
		},
	)

	assert.Equal(t, map[string]string{
		"k0": "v1",
		"k1": "v2",
	}, out)
}

func TestMergeDomainData_Nil(t *testing.T) {
	wh := &WorkflowHandler{}
	out := wh.mergeDomainData(
		nil,
		map[string]string{
			"k0": "v1",
			"k1": "v2",
		},
	)

	assert.Equal(t, map[string]string{
		"k0": "v1",
		"k1": "v2",
	}, out)
}

func TestDisableListVisibilityByFilter(t *testing.T) {
	logger := bark.NewNopLogger()
	domain := "test-domain"
	domainID := uuid.New()
	config := NewConfig(dc.NewCollection(dc.NewNopClient(), logger))
	config.DisableListVisibilityByFilter = dc.GetBoolPropertyFnFilteredByDomain(true)

	mockClusterMetadata := &mocks.ClusterMetadata{}
	mockProducer := &mocks.KafkaProducer{}
	mockMetricClient := metrics.NewClient(tally.NoopScope, metrics.Frontend)
	mockMessagingClient := mocks.NewMockMessagingClient(mockProducer, nil)
	baseService := cs.NewTestService(mockClusterMetadata, mockMessagingClient, mockMetricClient, logger)
	mockMetadataMgr := &mocks.MetadataManager{}
	mockHistoryMgr := &mocks.HistoryManager{}
	mockHistoryV2Mgr := &mocks.HistoryV2Manager{}
	mockVisibilityMgr := &mocks.VisibilityManager{}
	wh := NewWorkflowHandler(baseService, config, mockMetadataMgr, mockHistoryMgr, mockHistoryV2Mgr, mockVisibilityMgr, mockProducer)
	mockDomainCache := &cache.DomainCacheMock{}
	wh.metricsClient = wh.Service.GetMetricsClient()
	wh.domainCache = mockDomainCache
	wh.startWG.Done()

	mockDomainCache.On("GetDomainID", mock.Anything).Return(domainID, nil)

	// test list open by wid
	listRequest := &shared.ListOpenWorkflowExecutionsRequest{
		Domain: common.StringPtr(domain),
		StartTimeFilter: &shared.StartTimeFilter{
			EarliestTime: common.Int64Ptr(0),
			LatestTime:   common.Int64Ptr(time.Now().UnixNano()),
		},
		ExecutionFilter: &shared.WorkflowExecutionFilter{
			WorkflowId: common.StringPtr("wid"),
		},
	}
	_, err := wh.ListOpenWorkflowExecutions(context.Background(), listRequest)
	assert.Error(t, err)
	assert.Equal(t, errNoPermission, err)

	// test list open by workflow type
	listRequest.ExecutionFilter = nil
	listRequest.TypeFilter = &shared.WorkflowTypeFilter{
		Name: common.StringPtr("workflow-type"),
	}
	_, err = wh.ListOpenWorkflowExecutions(context.Background(), listRequest)
	assert.Error(t, err)
	assert.Equal(t, errNoPermission, err)

	// test list close by wid
	listRequest2 := &shared.ListClosedWorkflowExecutionsRequest{
		Domain: common.StringPtr(domain),
		StartTimeFilter: &shared.StartTimeFilter{
			EarliestTime: common.Int64Ptr(0),
			LatestTime:   common.Int64Ptr(time.Now().UnixNano()),
		},
		ExecutionFilter: &shared.WorkflowExecutionFilter{
			WorkflowId: common.StringPtr("wid"),
		},
	}
	_, err = wh.ListClosedWorkflowExecutions(context.Background(), listRequest2)
	assert.Error(t, err)
	assert.Equal(t, errNoPermission, err)

	// test list close by workflow type
	listRequest2.ExecutionFilter = nil
	listRequest2.TypeFilter = &shared.WorkflowTypeFilter{
		Name: common.StringPtr("workflow-type"),
	}
	_, err = wh.ListClosedWorkflowExecutions(context.Background(), listRequest2)
	assert.Error(t, err)
	assert.Equal(t, errNoPermission, err)

	// test list close by workflow status
	listRequest2.TypeFilter = nil
	failedStatus := shared.WorkflowExecutionCloseStatusFailed
	listRequest2.StatusFilter = &failedStatus
	_, err = wh.ListClosedWorkflowExecutions(context.Background(), listRequest2)
	assert.Error(t, err)
	assert.Equal(t, errNoPermission, err)
}
