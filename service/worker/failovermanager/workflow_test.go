// Copyright (c) 2017-2020 Uber Technologies Inc.
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

package failovermanager

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"go.uber.org/cadence/worker"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/resource"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/cadence/testsuite"
)

type failoverWorkflowTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
}

func TestFailoverWorkflowTestSuite(t *testing.T) {
	suite.Run(t, new(failoverWorkflowTestSuite))
}

func (s *failoverWorkflowTestSuite) TestValidateParams() {
	s.Error(validateParams(nil))
	params := &FailoverParams{}
	s.Error(validateParams(params))
	params.TargetCluster = "t"
	s.Error(validateParams(params))
	params.SourceCluster = "t"
	s.Error(validateParams(params))
	params.SourceCluster = "s"
	s.NoError(validateParams(params))
}

func (s *failoverWorkflowTestSuite) TestWorkflow_InvalidParams() {
	env := s.NewTestWorkflowEnvironment()
	params := &FailoverParams{}
	env.ExecuteWorkflow(WorkflowTypeName, params)
	s.True(env.IsWorkflowCompleted())
	s.Error(env.GetWorkflowError())
}

func (s *failoverWorkflowTestSuite) TestWorkflow_GetDomainActivityError() {
	env := s.NewTestWorkflowEnvironment()
	err := errors.New("mockErr")
	env.OnActivity(getDomainsActivityName, mock.Anything, mock.Anything).Return(nil, err)
	params := &FailoverParams{
		TargetCluster: "t",
		SourceCluster: "s",
	}
	env.ExecuteWorkflow(WorkflowTypeName, params)
	s.True(env.IsWorkflowCompleted())
	s.Equal("mockErr", env.GetWorkflowError().Error())
}

func (s *failoverWorkflowTestSuite) TestWorkflow_FailoverActivityError() {
	env := s.NewTestWorkflowEnvironment()
	domains := []string{"d1"}
	err := errors.New("mockErr")
	env.OnActivity(getDomainsActivityName, mock.Anything, mock.Anything).Return(domains, nil)
	env.OnActivity(failoverActivityName, mock.Anything, mock.Anything).Return(nil, err)
	params := &FailoverParams{
		TargetCluster: "t",
		SourceCluster: "s",
	}
	env.ExecuteWorkflow(WorkflowTypeName, params)
	var result FailoverResult
	s.NoError(env.GetWorkflowResult(&result))
	s.Equal(0, len(result.SuccessDomains))
	s.Equal(domains, result.FailedDomains)
}

func (s *failoverWorkflowTestSuite) TestWorkflow_Success() {
	env := s.NewTestWorkflowEnvironment()
	domains := []string{"d1"}
	mockFailoverActivityResult := &FailoverActivityResult{
		SuccessDomains: []string{"d1"},
	}
	env.OnActivity(getDomainsActivityName, mock.Anything, mock.Anything).Return(domains, nil)
	env.OnActivity(failoverActivityName, mock.Anything, mock.Anything).Return(mockFailoverActivityResult, nil)
	params := &FailoverParams{
		TargetCluster: "t",
		SourceCluster: "s",
	}
	env.ExecuteWorkflow(WorkflowTypeName, params)
	var result FailoverResult
	s.NoError(env.GetWorkflowResult(&result))
	s.Equal(mockFailoverActivityResult.SuccessDomains, result.SuccessDomains)
	s.Equal(mockFailoverActivityResult.FailedDomains, result.FailedDomains)

	queryResult, err := env.QueryWorkflow(QueryType)
	s.NoError(err)
	var res QueryResult
	s.NoError(queryResult.Get(&res))
	s.Equal(len(domains), res.TotalDomains)
	s.Equal(len(domains), res.Success)
	s.Equal(0, res.Failed)
	s.Equal(WorkflowCompleted, res.State)
	s.Equal("t", res.TargetCluster)
	s.Equal("s", res.SourceCluster)
	s.Equal(domains, res.SuccessDomains)
	s.Equal(0, len(res.FailedDomains))

}

func (s *failoverWorkflowTestSuite) TestWorkflow_Success_Batches() {
	env := s.NewTestWorkflowEnvironment()
	domains := []string{"d1", "d2", "d3"}
	expectFailoverActivityParams1 := &FailoverActivityParams{
		Domains:       []string{"d1", "d2"},
		TargetCluster: "t",
	}
	mockFailoverActivityResult1 := &FailoverActivityResult{
		SuccessDomains: []string{"d1", "d2"},
	}
	expectFailoverActivityParams2 := &FailoverActivityParams{
		Domains:       []string{"d3"},
		TargetCluster: "t",
	}
	mockFailoverActivityResult2 := &FailoverActivityResult{
		FailedDomains: []string{"d3"},
	}
	env.OnActivity(getDomainsActivityName, mock.Anything, mock.Anything).Return(domains, nil)
	env.OnActivity(failoverActivityName, mock.Anything, expectFailoverActivityParams1).Return(mockFailoverActivityResult1, nil).Once()
	env.OnActivity(failoverActivityName, mock.Anything, expectFailoverActivityParams2).Return(mockFailoverActivityResult2, nil).Once()

	params := &FailoverParams{
		TargetCluster:     "t",
		SourceCluster:     "s",
		BatchFailoverSize: 2,
		Domains:           domains,
	}
	env.ExecuteWorkflow(WorkflowTypeName, params)

	var result FailoverResult
	s.NoError(env.GetWorkflowResult(&result))
	s.Equal(mockFailoverActivityResult1.SuccessDomains, result.SuccessDomains)
	s.Equal(mockFailoverActivityResult2.FailedDomains, result.FailedDomains)
}

func (s *failoverWorkflowTestSuite) TestWorkflow_Pause() {
	env := s.NewTestWorkflowEnvironment()
	domains := []string{"d1"}
	mockFailoverActivityResult := &FailoverActivityResult{
		SuccessDomains: []string{"d1"},
	}
	env.OnActivity(getDomainsActivityName, mock.Anything, mock.Anything).Return(domains, nil)
	env.OnActivity(failoverActivityName, mock.Anything, mock.Anything).Return(mockFailoverActivityResult, nil).Once()

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(PauseSignal, nil)
	}, time.Millisecond*0)
	env.RegisterDelayedCallback(func() {
		s.assertQueryState(env, WorkflowPaused)
	}, time.Millisecond*100)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(ResumeSignal, nil)
	}, time.Millisecond*200)
	env.RegisterDelayedCallback(func() {
		s.assertQueryState(env, WorkflowRunning)
	}, time.Millisecond*300)

	params := &FailoverParams{
		TargetCluster:     "t",
		SourceCluster:     "s",
		BatchFailoverSize: 2,
		Domains:           domains,
	}
	env.ExecuteWorkflow(WorkflowTypeName, params)

	var result FailoverResult
	s.NoError(env.GetWorkflowResult(&result))
	s.Equal(mockFailoverActivityResult.SuccessDomains, result.SuccessDomains)
}

func (s *failoverWorkflowTestSuite) assertQueryState(env *testsuite.TestWorkflowEnvironment, expectedState string) {
	queryResult, err := env.QueryWorkflow(QueryType)
	s.NoError(err)
	var res QueryResult
	s.NoError(queryResult.Get(&res))
	s.Equal(expectedState, res.State)
}

var clusters = []*shared.ClusterReplicationConfiguration{
	{
		ClusterName: common.StringPtr("c1"),
	},
	{
		ClusterName: common.StringPtr("c2"),
	},
}

func (s *failoverWorkflowTestSuite) TestShouldFailover() {

	tests := []struct {
		domain        *shared.DescribeDomainResponse
		sourceCluster string
		expected      bool
	}{
		{
			domain: &shared.DescribeDomainResponse{
				IsGlobalDomain: common.BoolPtr(false),
			},
			sourceCluster: "c1",
			expected:      false,
		},
		{
			domain: &shared.DescribeDomainResponse{
				IsGlobalDomain: common.BoolPtr(true),
				ReplicationConfiguration: &shared.DomainReplicationConfiguration{
					ActiveClusterName: common.StringPtr("c1"),
					Clusters:          clusters,
				},
			},
			sourceCluster: "c2",
			expected:      false,
		},
		{
			domain: &shared.DescribeDomainResponse{
				IsGlobalDomain: common.BoolPtr(true),
				ReplicationConfiguration: &shared.DomainReplicationConfiguration{
					ActiveClusterName: common.StringPtr("c2"),
					Clusters:          clusters,
				},
			},
			sourceCluster: "c2",
			expected:      false,
		},
		{
			domain: &shared.DescribeDomainResponse{
				IsGlobalDomain: common.BoolPtr(true),
				ReplicationConfiguration: &shared.DomainReplicationConfiguration{
					ActiveClusterName: common.StringPtr("c2"),
					Clusters:          clusters,
				},
				DomainInfo: &shared.DomainInfo{
					Data: map[string]string{
						common.DomainDataKeyForManagedFailover: "true",
					},
				},
			},
			sourceCluster: "c2",
			expected:      true,
		},
	}
	for _, t := range tests {
		s.Equal(t.expected, shouldFailover(t.domain, t.sourceCluster))
	}
}

func (s *failoverWorkflowTestSuite) prepareTestActivityEnv() (*testsuite.TestActivityEnvironment, *resource.Test, *gomock.Controller) {
	env := s.NewTestActivityEnvironment()
	controller := gomock.NewController(s.T())
	mockResource := resource.NewTest(controller, metrics.Worker)

	ctx := &FailoverManager{
		svcClient:  mockResource.GetSDKClient(),
		clientBean: mockResource.ClientBean,
	}
	env.SetTestTimeout(time.Second * 5)
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), failoverManagerContextKey, ctx),
	})
	return env, mockResource, controller
}
func (s *failoverWorkflowTestSuite) TestGetDomainsActivity() {
	env, mockResource, controller := s.prepareTestActivityEnv()
	defer controller.Finish()
	defer mockResource.Finish(s.T())

	domains := &shared.ListDomainsResponse{
		Domains: []*shared.DescribeDomainResponse{
			{
				DomainInfo: &shared.DomainInfo{
					Name: common.StringPtr("d1"),
					Data: map[string]string{common.DomainDataKeyForManagedFailover: "true"},
				},
				ReplicationConfiguration: &shared.DomainReplicationConfiguration{
					ActiveClusterName: common.StringPtr("c1"),
					Clusters:          clusters,
				},
				IsGlobalDomain: common.BoolPtr(true),
			},
		},
	}
	mockResource.FrontendClient.EXPECT().ListDomains(gomock.Any(), gomock.Any()).Return(domains, nil)

	params := &GetDomainsActivityParams{
		TargetCluster: "c2",
		SourceCluster: "c1",
	}
	actResult, err := env.ExecuteActivity(getDomainsActivityName, params)
	s.NoError(err)
	var result []string
	s.NoError(actResult.Get(&result))
	s.Equal([]string{"d1"}, result)
}

func (s *failoverWorkflowTestSuite) TestGetDomainsActivity_WithTargetDomains() {
	env, mockResource, controller := s.prepareTestActivityEnv()
	defer controller.Finish()
	defer mockResource.Finish(s.T())

	domains := &shared.ListDomainsResponse{
		Domains: []*shared.DescribeDomainResponse{
			{
				DomainInfo: &shared.DomainInfo{
					Name: common.StringPtr("d1"),
					Data: map[string]string{common.DomainDataKeyForManagedFailover: "true"},
				},
				ReplicationConfiguration: &shared.DomainReplicationConfiguration{
					ActiveClusterName: common.StringPtr("c1"),
					Clusters:          clusters,
				},
				IsGlobalDomain: common.BoolPtr(true),
			},
			{
				DomainInfo: &shared.DomainInfo{
					Name: common.StringPtr("d2"),
					Data: map[string]string{common.DomainDataKeyForManagedFailover: "true"},
				},
				ReplicationConfiguration: &shared.DomainReplicationConfiguration{
					ActiveClusterName: common.StringPtr("c1"),
					Clusters:          clusters,
				},
				IsGlobalDomain: common.BoolPtr(true),
			},
			{
				DomainInfo: &shared.DomainInfo{
					Name: common.StringPtr("d3"),
				},
				ReplicationConfiguration: &shared.DomainReplicationConfiguration{
					ActiveClusterName: common.StringPtr("c1"),
					Clusters:          clusters,
				},
				IsGlobalDomain: common.BoolPtr(true),
			},
		},
	}
	mockResource.FrontendClient.EXPECT().ListDomains(gomock.Any(), gomock.Any()).Return(domains, nil)

	params := &GetDomainsActivityParams{
		TargetCluster: "c2",
		SourceCluster: "c1",
		Domains:       []string{"d1", "d3"}, // only target d1 and d3
	}
	actResult, err := env.ExecuteActivity(getDomainsActivityName, params)
	s.NoError(err)
	var result []string
	s.NoError(actResult.Get(&result))
	s.Equal([]string{"d1"}, result) // d3 filtered out because not managed
}

func (s *failoverWorkflowTestSuite) TestFailoverActivity() {
	env, mockResource, controller := s.prepareTestActivityEnv()
	defer controller.Finish()
	defer mockResource.Finish(s.T())

	domains := []string{"d1", "d2"}
	mockResource.FrontendClient.EXPECT().UpdateDomain(gomock.Any(), gomock.Any()).Return(nil, nil).Times(len(domains))

	params := &FailoverActivityParams{
		Domains:       domains,
		TargetCluster: "c2",
	}

	actResult, err := env.ExecuteActivity(failoverActivityName, params)
	s.NoError(err)
	var result FailoverActivityResult
	s.NoError(actResult.Get(&result))
	s.Equal(domains, result.SuccessDomains)
}

func (s *failoverWorkflowTestSuite) TestFailoverActivity_Error() {
	env, mockResource, controller := s.prepareTestActivityEnv()
	defer controller.Finish()
	defer mockResource.Finish(s.T())

	domains := []string{"d1", "d2"}
	targetCluster := "c2"
	replicationConfig := &shared.DomainReplicationConfiguration{
		ActiveClusterName: common.StringPtr(targetCluster),
	}
	updateRequest1 := &shared.UpdateDomainRequest{
		Name:                     common.StringPtr("d1"),
		ReplicationConfiguration: replicationConfig,
	}
	updateRequest2 := &shared.UpdateDomainRequest{
		Name:                     common.StringPtr("d2"),
		ReplicationConfiguration: replicationConfig,
	}
	mockResource.FrontendClient.EXPECT().UpdateDomain(gomock.Any(), updateRequest1).Return(nil, nil)
	mockResource.FrontendClient.EXPECT().UpdateDomain(gomock.Any(), updateRequest2).Return(nil, errors.New("mockErr"))

	params := &FailoverActivityParams{
		Domains:       domains,
		TargetCluster: targetCluster,
	}

	actResult, err := env.ExecuteActivity(failoverActivityName, params)
	s.NoError(err)
	var result FailoverActivityResult
	s.NoError(actResult.Get(&result))
	s.Equal([]string{"d1"}, result.SuccessDomains)
	s.Equal([]string{"d2"}, result.FailedDomains)
}
