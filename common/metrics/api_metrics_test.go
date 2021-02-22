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

package metrics

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	apiMetricsSuite struct {
		suite.Suite
		*require.Assertions
	}
)

func TestAPIMetricsSuite(t *testing.T) {
	s := new(apiMetricsSuite)
	suite.Run(t, s)
}

func (s *apiMetricsSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *apiMetricsSuite) TearDownTest() {

}

func (s *apiMetricsSuite) TestFrontendAPIMetrics() {
	apiNames := FrontendAPIMetricsNames()
	apiNameToScope := FrontendAPIMetricsScopes()
	for apiName := range apiNames {
		if _, ok := apiNameToScope[apiName]; !ok {
			fmt.Println(apiName)
		}
	}
	s.Equal(len(apiNames), len(apiNameToScope))
}

func (s *apiMetricsSuite) TestMatchingAPIMetrics() {
	apiNames := MatchingAPIMetricsNames()
	apiNameToScope := MatchingAPIMetricsScopes()
	for apiName := range apiNames {
		if _, ok := apiNameToScope[apiName]; !ok {
			fmt.Println(apiName)
		}
	}
	s.Equal(len(apiNames), len(apiNameToScope))
}

func (s *apiMetricsSuite) TestHistoryAPIMetrics() {
	apiNames := HistoryAPIMetricsNames()
	apiNameToScope := HistoryAPIMetricsScopes()
	for apiName := range apiNames {
		if _, ok := apiNameToScope[apiName]; !ok {
			fmt.Println(apiName)
		}
	}
	s.Equal(len(apiNames), len(apiNameToScope))
}
