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

package client

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/testing/temporalapi"
	"golang.org/x/exp/slices"

	"go.temporal.io/api/workflowservice/v1"
)

type (
	quotasSuite struct {
		suite.Suite
		*require.Assertions
	}
)

func TestQuotasSuite(t *testing.T) {
	s := new(quotasSuite)
	suite.Run(t, s)
}

func (s *quotasSuite) SetupSuite() {
}

func (s *quotasSuite) TearDownSuite() {
}

func (s *quotasSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *quotasSuite) TearDownTest() {
}

func (s *quotasSuite) TestCallerTypeDefaultPriorityMapping() {
	for _, priority := range CallerTypeDefaultPriority {
		index := slices.Index(RequestPrioritiesOrdered, priority)
		s.NotEqual(-1, index)
	}
}

func (s *quotasSuite) TestAPITypeCallOriginPriorityOverrideMapping() {
	for _, priority := range APITypeCallOriginPriorityOverride {
		index := slices.Index(RequestPrioritiesOrdered, priority)
		s.NotEqual(-1, index)
	}
}

func (s *quotasSuite) TestBackgroundTypeAPIPriorityOverrideMapping() {
	for _, priority := range BackgroundTypeAPIPriorityOverride {
		index := slices.Index(RequestPrioritiesOrdered, priority)
		s.NotEqual(-1, index)
	}
}

func (s *quotasSuite) TestRequestPrioritiesOrdered() {
	for idx := range RequestPrioritiesOrdered[1:] {
		s.True(RequestPrioritiesOrdered[idx] < RequestPrioritiesOrdered[idx+1])
	}
}

func (s *quotasSuite) TestCallOriginDefined() {
	var service workflowservice.WorkflowServiceServer
	definedAPIs := make(map[string]struct{})
	temporalapi.WalkExportedMethods(&service, func(m reflect.Method) {
		definedAPIs[m.Name] = struct{}{}
	})

	for api := range APITypeCallOriginPriorityOverride {
		_, ok := definedAPIs[api]
		s.True(ok)
	}
}

func (s *quotasSuite) TestPriorityNamespaceRateLimiter_DoesLimit() {
	namespaceMaxRPS := func(namespace string) int { return 1 }
	hostMaxRPS := func() int { return 1 }
	operatorRPSRatioFn := func() float64 { return 0.2 }
	burstRatio := func() float64 { return 1 }

	limiter := newPriorityNamespaceRateLimiter(
		namespaceMaxRPS,
		hostMaxRPS,
		RequestPriorityFn,
		operatorRPSRatioFn,
		burstRatio,
	)

	request := quotas.NewRequest(
		"test-api",
		1,
		"test-namespace",
		"api",
		-1,
		"frontend",
	)

	requestTime := time.Now()
	wasLimited := false

	for i := 0; i < 2; i++ {
		if !limiter.Allow(requestTime, request) {
			wasLimited = true
		}
	}

	s.True(wasLimited)
}

func (s *quotasSuite) TestPerShardNamespaceRateLimiter_DoesLimit() {
	perShardNamespaceMaxRPS := func(namespace string) int { return 1 }
	hostMaxRPS := func() int { return 1 }
	operatorRPSRatioFn := func() float64 { return 0.2 }
	burstRatio := func() float64 { return 1 }

	limiter := newPerShardPerNamespacePriorityRateLimiter(
		perShardNamespaceMaxRPS,
		hostMaxRPS,
		RequestPriorityFn,
		operatorRPSRatioFn,
		burstRatio,
	)

	request := quotas.NewRequest(
		"test-api",
		1,
		"test-namespace",
		"api",
		1,
		"frontend",
	)

	requestTime := time.Now()
	wasLimited := false

	for i := 0; i < 2; i++ {
		if !limiter.Allow(requestTime, request) {
			wasLimited = true
		}
	}

	s.True(wasLimited)
}

func (s *quotasSuite) TestOperatorPrioritized() {
	rateFn := func() float64 { return 5 }
	operatorRPSRatioFn := func() float64 { return 0.2 }
	burstRatio := func() float64 { return 1 }
	limiter := newPriorityRateLimiter(
		rateFn,
		RequestPriorityFn,
		operatorRPSRatioFn,
		burstRatio,
	)

	operatorRequest := quotas.NewRequest(
		"DescribeWorkflowExecution",
		1,
		"test-namespace",
		headers.CallerTypeOperator,
		-1,
		"DescribeWorkflowExecution")

	apiRequest := quotas.NewRequest(
		"DescribeWorkflowExecution",
		1,
		"test-namespace",
		headers.CallerTypeAPI,
		-1,
		"DescribeWorkflowExecution")

	requestTime := time.Now()
	wasLimited := false

	for i := 0; i < 6; i++ {
		if !limiter.Allow(requestTime, apiRequest) {
			wasLimited = true
			s.True(limiter.Allow(requestTime, operatorRequest))
		}
	}
	s.True(wasLimited)
}
