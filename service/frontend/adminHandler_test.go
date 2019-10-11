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
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	"github.com/uber/cadence/.gen/go/admin"
	"github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/.gen/go/history/historyservicetest"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service"
)

type (
	adminHandlerSuite struct {
		suite.Suite
		logger                 log.Logger
		domainName             string
		domainID               string
		currentClusterName     string
		alternativeClusterName string
		service                service.Service
		domainCache            *cache.DomainCacheMock

		controller          *gomock.Controller
		mockClusterMetadata *mocks.ClusterMetadata
		mockClientBean      *client.MockClientBean
		mockHistoryV2Mgr    *mocks.HistoryV2Manager
		historyClient       *historyservicetest.MockClient

		handler *AdminHandler
	}
)

func TestAdminHandlerSuite(t *testing.T) {
	s := new(adminHandlerSuite)
	suite.Run(t, s)
}

func (s *adminHandlerSuite) SetupTest() {
	var err error
	s.logger, err = loggerimpl.NewDevelopment()
	s.Require().NoError(err)
	s.domainName = "some random domain name"
	s.domainID = "some random domain ID"
	s.currentClusterName = cluster.TestCurrentClusterName
	s.alternativeClusterName = cluster.TestAlternativeClusterName

	s.mockClusterMetadata = &mocks.ClusterMetadata{}
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(s.currentClusterName)
	s.mockClusterMetadata.On("IsGlobalDomainEnabled").Return(true)
	metricsClient := metrics.NewClient(tally.NoopScope, metrics.Frontend)
	s.mockClientBean = &client.MockClientBean{}
	s.controller = gomock.NewController(s.T())
	s.historyClient = historyservicetest.NewMockClient(s.controller)
	s.mockClientBean.On("GetHistoryClient").Return(s.historyClient)
	s.service = service.NewTestService(s.mockClusterMetadata, nil, metricsClient, s.mockClientBean, nil, nil, nil)
	s.domainCache = &cache.DomainCacheMock{}
	s.domainCache.On("Start").Return()
	s.domainCache.On("Stop").Return()
	s.mockHistoryV2Mgr = &mocks.HistoryV2Manager{}
	s.handler = NewAdminHandler(s.service, 1, s.domainCache, s.mockHistoryV2Mgr, nil)
	s.handler.Start()
}

func (s *adminHandlerSuite) TearDownTest() {
	s.controller.Finish()
	s.handler.Stop()
}

func (s *adminHandlerSuite) Test_ConvertIndexedValueTypeToESDataType() {
	tests := []struct {
		input    shared.IndexedValueType
		expected string
	}{
		{
			input:    shared.IndexedValueTypeString,
			expected: "text",
		},
		{
			input:    shared.IndexedValueTypeKeyword,
			expected: "keyword",
		},
		{
			input:    shared.IndexedValueTypeInt,
			expected: "long",
		},
		{
			input:    shared.IndexedValueTypeDouble,
			expected: "double",
		},
		{
			input:    shared.IndexedValueTypeBool,
			expected: "boolean",
		},
		{
			input:    shared.IndexedValueTypeDatetime,
			expected: "date",
		},
		{
			input:    shared.IndexedValueType(-1),
			expected: "",
		},
	}

	for _, test := range tests {
		s.Equal(test.expected, convertIndexedValueTypeToESDataType(test.input))
	}
}

func (s *adminHandlerSuite) Test_GetWorkflowExecutionRawHistoryV2_FailedOnInvalidWorkflowID() {

	ctx := context.Background()
	_, err := s.handler.GetWorkflowExecutionRawHistoryV2(ctx,
		&admin.GetWorkflowExecutionRawHistoryV2Request{
			Domain: common.StringPtr(s.domainName),
			Execution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(""),
				RunId:      common.StringPtr(uuid.New()),
			},
			StartEventId:      common.Int64Ptr(1),
			StartEventVersion: common.Int64Ptr(100),
			EndEventId:        common.Int64Ptr(10),
			EndEventVersion:   common.Int64Ptr(100),
			MaximumPageSize:   common.Int32Ptr(1),
			NextPageToken:     nil,
		})
	s.Error(err)
}

func (s *adminHandlerSuite) Test_GetWorkflowExecutionRawHistoryV2_FailedOnInvalidRunID() {
	ctx := context.Background()
	_, err := s.handler.GetWorkflowExecutionRawHistoryV2(ctx,
		&admin.GetWorkflowExecutionRawHistoryV2Request{
			Domain: common.StringPtr(s.domainName),
			Execution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr("workflowID"),
				RunId:      common.StringPtr("runID"),
			},
			StartEventId:      common.Int64Ptr(1),
			StartEventVersion: common.Int64Ptr(100),
			EndEventId:        common.Int64Ptr(10),
			EndEventVersion:   common.Int64Ptr(100),
			MaximumPageSize:   common.Int32Ptr(1),
			NextPageToken:     nil,
		})
	s.Error(err)
}

func (s *adminHandlerSuite) Test_GetWorkflowExecutionRawHistoryV2_FailedOnInvalidSize() {
	ctx := context.Background()
	_, err := s.handler.GetWorkflowExecutionRawHistoryV2(ctx,
		&admin.GetWorkflowExecutionRawHistoryV2Request{
			Domain: common.StringPtr(s.domainName),
			Execution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr("workflowID"),
				RunId:      common.StringPtr(uuid.New()),
			},
			StartEventId:      common.Int64Ptr(1),
			StartEventVersion: common.Int64Ptr(100),
			EndEventId:        common.Int64Ptr(10),
			EndEventVersion:   common.Int64Ptr(100),
			MaximumPageSize:   common.Int32Ptr(-1),
			NextPageToken:     nil,
		})
	s.Error(err)
}

func (s *adminHandlerSuite) Test_GetWorkflowExecutionRawHistoryV2_FailedOnDomainCache() {
	ctx := context.Background()
	s.domainCache.On("GetDomainID", s.domainName).Return("", fmt.Errorf("test"))
	_, err := s.handler.GetWorkflowExecutionRawHistoryV2(ctx,
		&admin.GetWorkflowExecutionRawHistoryV2Request{
			Domain: common.StringPtr(s.domainName),
			Execution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr("workflowID"),
				RunId:      common.StringPtr(uuid.New()),
			},
			StartEventId:      common.Int64Ptr(1),
			StartEventVersion: common.Int64Ptr(100),
			EndEventId:        common.Int64Ptr(10),
			EndEventVersion:   common.Int64Ptr(100),
			MaximumPageSize:   common.Int32Ptr(1),
			NextPageToken:     nil,
		})
	s.Error(err)
}

func (s *adminHandlerSuite) Test_GetWorkflowExecutionRawHistoryV2() {
	ctx := context.Background()
	s.domainCache.On("GetDomainID", s.domainName).Return(s.domainID, nil)
	branchToken := []byte{1}
	versionHistory := persistence.NewVersionHistory(branchToken, []*persistence.VersionHistoryItem{
		persistence.NewVersionHistoryItem(int64(10), int64(100)),
	})
	rawVersionHistories := persistence.NewVersionHistories(versionHistory)
	versionHistories := rawVersionHistories.ToThrift()
	mState := &history.GetMutableStateResponse{
		NextEventId:        common.Int64Ptr(11),
		CurrentBranchToken: branchToken,
		VersionHistories:   versionHistories,
		ReplicationInfo:    make(map[string]*shared.ReplicationInfo),
	}
	s.historyClient.EXPECT().GetMutableState(gomock.Any(), gomock.Any()).Return(mState, nil).AnyTimes()

	s.mockHistoryV2Mgr.On("ReadRawHistoryBranch", mock.Anything).Return(&persistence.ReadRawHistoryBranchResponse{
		HistoryEventBlobs: []*persistence.DataBlob{},
		NextPageToken:     []byte{},
		Size:              0,
	}, nil)
	_, err := s.handler.GetWorkflowExecutionRawHistoryV2(ctx,
		&admin.GetWorkflowExecutionRawHistoryV2Request{
			Domain: common.StringPtr(s.domainName),
			Execution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr("workflowID"),
				RunId:      common.StringPtr(uuid.New()),
			},
			StartEventId:      common.Int64Ptr(1),
			StartEventVersion: common.Int64Ptr(100),
			EndEventId:        common.Int64Ptr(10),
			EndEventVersion:   common.Int64Ptr(100),
			MaximumPageSize:   common.Int32Ptr(10),
			NextPageToken:     nil,
		})
	s.NoError(err)
}

func (s *adminHandlerSuite) Test_GetEventRange_DefinedStartAndEnd() {
	inputStartEventID := int64(1)
	inputStartVersion := int64(10)
	inputEndEventID := int64(100)
	inputEndVersion := int64(11)
	firstItem := persistence.NewVersionHistoryItem(inputStartEventID, inputStartVersion)
	endItem := persistence.NewVersionHistoryItem(inputEndEventID, inputEndVersion)
	versionHistory := persistence.NewVersionHistory([]byte{}, []*persistence.VersionHistoryItem{firstItem, endItem})
	versionHistories := persistence.NewVersionHistories(versionHistory)
	request := &admin.GetWorkflowExecutionRawHistoryV2Request{
		Domain: common.StringPtr(s.domainName),
		Execution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr("workflowID"),
			RunId:      common.StringPtr(uuid.New()),
		},
		StartEventId:      common.Int64Ptr(inputStartEventID),
		StartEventVersion: common.Int64Ptr(inputStartVersion),
		EndEventId:        common.Int64Ptr(inputEndEventID),
		EndEventVersion:   common.Int64Ptr(inputEndVersion),
		MaximumPageSize:   common.Int32Ptr(10),
		NextPageToken:     nil,
	}

	targetVersionHistory, err := s.handler.updateEventRange(
		request,
		versionHistories,
	)
	s.Equal(request.GetStartEventId(), inputStartEventID)
	s.Equal(request.GetEndEventId(), inputEndEventID)
	s.Equal(targetVersionHistory, versionHistory)
	s.NoError(err)
}

func (s *adminHandlerSuite) Test_GetEventRange_DefinedEndEvent() {
	inputStartEventID := int64(1)
	inputEndEventID := int64(100)
	inputStartVersion := int64(10)
	inputEndVersion := int64(11)
	firstItem := persistence.NewVersionHistoryItem(inputStartEventID, inputStartVersion)
	targetItem := persistence.NewVersionHistoryItem(inputEndEventID, inputEndVersion)
	versionHistory := persistence.NewVersionHistory([]byte{}, []*persistence.VersionHistoryItem{firstItem, targetItem})
	versionHistories := persistence.NewVersionHistories(versionHistory)
	request := &admin.GetWorkflowExecutionRawHistoryV2Request{
		Domain: common.StringPtr(s.domainName),
		Execution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr("workflowID"),
			RunId:      common.StringPtr(uuid.New()),
		},
		StartEventId:      nil,
		StartEventVersion: nil,
		EndEventId:        common.Int64Ptr(inputEndEventID),
		EndEventVersion:   common.Int64Ptr(inputEndVersion),
		MaximumPageSize:   common.Int32Ptr(10),
		NextPageToken:     nil,
	}

	targetVersionHistory, err := s.handler.updateEventRange(
		request,
		versionHistories,
	)
	s.Equal(request.GetStartEventId(), inputStartEventID-1)
	s.Equal(request.GetEndEventId(), inputEndEventID)
	s.Equal(targetVersionHistory, versionHistory)
	s.NoError(err)
}

func (s *adminHandlerSuite) Test_GetEventRange_DefinedStartEvent() {
	inputStartEventID := int64(1)
	inputEndEventID := int64(100)
	inputStartVersion := int64(10)
	inputEndVersion := int64(11)
	firstItem := persistence.NewVersionHistoryItem(inputStartEventID, inputStartVersion)
	targetItem := persistence.NewVersionHistoryItem(inputEndEventID, inputEndVersion)
	versionHistory := persistence.NewVersionHistory([]byte{}, []*persistence.VersionHistoryItem{firstItem, targetItem})
	versionHistories := persistence.NewVersionHistories(versionHistory)
	request := &admin.GetWorkflowExecutionRawHistoryV2Request{
		Domain: common.StringPtr(s.domainName),
		Execution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr("workflowID"),
			RunId:      common.StringPtr(uuid.New()),
		},
		StartEventId:      common.Int64Ptr(inputStartEventID),
		StartEventVersion: common.Int64Ptr(inputStartVersion),
		EndEventId:        nil,
		EndEventVersion:   nil,
		MaximumPageSize:   common.Int32Ptr(10),
		NextPageToken:     nil,
	}

	targetVersionHistory, err := s.handler.updateEventRange(
		request,
		versionHistories,
	)
	s.Equal(request.GetStartEventId(), inputStartEventID)
	s.Equal(request.GetEndEventId(), inputEndEventID+1)
	s.Equal(targetVersionHistory, versionHistory)
	s.NoError(err)
}

func (s *adminHandlerSuite) Test_GetEventRange_NonCurrentBranch() {
	inputStartEventID := int64(1)
	inputEndEventID := int64(100)
	inputStartVersion := int64(10)
	inputEndVersion := int64(101)
	item1 := persistence.NewVersionHistoryItem(inputStartEventID, inputStartVersion)
	item2 := persistence.NewVersionHistoryItem(inputEndEventID, inputEndVersion)
	versionHistory1 := persistence.NewVersionHistory([]byte{}, []*persistence.VersionHistoryItem{item1, item2})
	item3 := persistence.NewVersionHistoryItem(int64(10), int64(20))
	item4 := persistence.NewVersionHistoryItem(int64(20), int64(51))
	versionHistory2 := persistence.NewVersionHistory([]byte{}, []*persistence.VersionHistoryItem{item1, item3, item4})
	versionHistories := persistence.NewVersionHistories(versionHistory1)
	_, _, err := versionHistories.AddVersionHistory(versionHistory2)
	s.NoError(err)
	request := &admin.GetWorkflowExecutionRawHistoryV2Request{
		Domain: common.StringPtr(s.domainName),
		Execution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr("workflowID"),
			RunId:      common.StringPtr(uuid.New()),
		},
		StartEventId:      common.Int64Ptr(9),
		StartEventVersion: common.Int64Ptr(20),
		EndEventId:        common.Int64Ptr(inputEndEventID),
		EndEventVersion:   common.Int64Ptr(inputEndVersion),
		MaximumPageSize:   common.Int32Ptr(10),
		NextPageToken:     nil,
	}

	targetVersionHistory, err := s.handler.updateEventRange(
		request,
		versionHistories,
	)
	s.Equal(request.GetStartEventId(), inputStartEventID)
	s.Equal(request.GetEndEventId(), inputEndEventID)
	s.Equal(targetVersionHistory, versionHistory1)
	s.NoError(err)
}
