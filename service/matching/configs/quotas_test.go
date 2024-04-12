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

package configs

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"golang.org/x/exp/slices"

	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/testing/temporalapi"
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

func (s *quotasSuite) TestAPIToPriorityMapping() {
	for _, priority := range APIToPriority {
		index := slices.Index(APIPrioritiesOrdered, priority)
		s.NotEqual(-1, index)
	}
}

func (s *quotasSuite) TestAPIPrioritiesOrdered() {
	for idx := range APIPrioritiesOrdered[1:] {
		s.True(APIPrioritiesOrdered[idx] < APIPrioritiesOrdered[idx+1])
	}
}

func (s *quotasSuite) TestAPIs() {
	var service matchingservice.MatchingServiceServer
	apiToPriority := make(map[string]int)
	temporalapi.WalkExportedMethods(&service, func(m reflect.Method) {
		fullName := "/temporal.server.api.matchingservice.v1.MatchingService/" + m.Name
		apiToPriority[fullName] = APIToPriority[fullName]
	})
	s.Equal(apiToPriority, APIToPriority)
}

func (s *quotasSuite) TestOperatorPrioritized() {
	rateFn := func() float64 { return 5 }
	operatorRPSRatioFn := func() float64 { return 0.2 }
	limiter := NewPriorityRateLimiter(rateFn, operatorRPSRatioFn)

	operatorRequest := quotas.NewRequest(
		"/temporal.server.api.matchingservice.v1.MatchingService/QueryWorkflow",
		1,
		"",
		headers.CallerTypeOperator,
		-1,
		"")

	apiRequest := quotas.NewRequest(
		"/temporal.server.api.matchingservice.v1.MatchingService/QueryWorkflow",
		1,
		"",
		headers.CallerTypeAPI,
		-1,
		"")

	requestTime := time.Now()
	limitCount := 0

	for i := 0; i < 12; i++ {
		if !limiter.Allow(requestTime, apiRequest) {
			limitCount++
			s.True(limiter.Allow(requestTime, operatorRequest))
		}
	}
	s.Equal(2, limitCount)
}
