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

package history

import (
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
	gen "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/metrics"
	mmocks "github.com/uber/cadence/common/metrics/mocks"
	"github.com/uber/cadence/common/mocks"
	p "github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/dynamicconfig"
	"testing"
	"time"
)

type VisibilitySamplingSuite struct {
	*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
	suite.Suite
	client       p.VisibilityManager
	persistence  *mocks.VisibilityManager
	metricClient *mmocks.Client
}

var (
	testDomainUUID        = "fb15e4b5-356f-466d-8c6d-a29223e5c536"
	testDomain            = "test-domain-name"
	testWorkflowExecution = gen.WorkflowExecution{
		WorkflowId: common.StringPtr("visibility-workflow-test"),
		RunId:      common.StringPtr("843f6fc7-102a-4c63-a2d4-7c653b01bf52"),
	}
	testWorkflowTypeName = "visibility-workflow"
)

func TestVisibilitySamplingSuite(t *testing.T) {
	suite.Run(t, new(VisibilitySamplingSuite))
}

func (s *VisibilitySamplingSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil

	s.persistence = &mocks.VisibilityManager{}
	config := &Config{
		VisibilityOpenMaxQPS:   dynamicconfig.GetIntPropertyFilteredByDomain(10),
		VisibilityClosedMaxQPS: dynamicconfig.GetIntPropertyFilteredByDomain(10),
	}
	s.metricClient = &mmocks.Client{}
	s.client = NewVisibilitySamplingClient(s.persistence, config, s.metricClient, bark.NewNopLogger())
}

func (s *VisibilitySamplingSuite) TearDownTest() {
	s.persistence.AssertExpectations(s.T())
	s.metricClient.AssertExpectations(s.T())
}

func (s *VisibilitySamplingSuite) TestRecordWorkflowExecutionStarted() {
	request := &p.RecordWorkflowExecutionStartedRequest{
		DomainUUID:       testDomainUUID,
		Domain:           testDomain,
		Execution:        testWorkflowExecution,
		WorkflowTypeName: testWorkflowTypeName,
		StartTimestamp:   time.Now().UnixNano(),
	}
	s.persistence.On("RecordWorkflowExecutionStarted", request).Return(nil).Once()
	s.NoError(s.client.RecordWorkflowExecutionStarted(request))

	// no remaining tokens
	s.metricClient.On("IncCounter", metrics.PersistenceRecordWorkflowExecutionStartedScope, metrics.PersistenceSampledCounter).Once()
	s.NoError(s.client.RecordWorkflowExecutionStarted(request))
}

func (s *VisibilitySamplingSuite) TestRecordWorkflowExecutionClosed() {
	request := &p.RecordWorkflowExecutionClosedRequest{
		DomainUUID:       testDomainUUID,
		Domain:           testDomain,
		Execution:        testWorkflowExecution,
		WorkflowTypeName: testWorkflowTypeName,
		Status:           gen.WorkflowExecutionCloseStatusCompleted,
	}
	request2 := &p.RecordWorkflowExecutionClosedRequest{
		DomainUUID:       testDomainUUID,
		Domain:           testDomain,
		Execution:        testWorkflowExecution,
		WorkflowTypeName: testWorkflowTypeName,
		Status:           gen.WorkflowExecutionCloseStatusFailed,
	}

	s.persistence.On("RecordWorkflowExecutionClosed", request).Return(nil).Once()
	s.NoError(s.client.RecordWorkflowExecutionClosed(request))
	s.persistence.On("RecordWorkflowExecutionClosed", request2).Return(nil).Once()
	s.NoError(s.client.RecordWorkflowExecutionClosed(request2))

	// no remaining tokens
	s.metricClient.On("IncCounter", metrics.PersistenceRecordWorkflowExecutionClosedScope, metrics.PersistenceSampledCounter).Once()
	s.NoError(s.client.RecordWorkflowExecutionClosed(request))
	s.metricClient.On("IncCounter", metrics.PersistenceRecordWorkflowExecutionClosedScope, metrics.PersistenceSampledCounter).Once()
	s.NoError(s.client.RecordWorkflowExecutionClosed(request2))
}
