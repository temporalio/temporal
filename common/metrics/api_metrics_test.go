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
	"sort"
	"strings"
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

func (s *apiMetricsSuite) assertAllAPIsHaveMetricsDefined(apiNames map[string]string, scopeDef map[string]int) {
	var missingAPIs []string
	for _, fullName := range apiNames {
		if _, ok := scopeDef[fullName]; !ok {
			missingAPIs = append(missingAPIs, fullName)
		}
	}
	if len(missingAPIs) > 0 {
		sort.Strings(missingAPIs)
		// You are here most likely because you added new API without defining metrics scope for them.
		s.Fail(
			"Missing metrics scope for APIs.",
			"Missing metrics scope for below APIs:\n%s. \nPlease define them in common/metrics/def.go",
			strings.Join(missingAPIs, ",\n"))
	}
}

func (s *apiMetricsSuite) TestFrontendAPIMetrics() {
	apiNames := frontendAPIMetricsNames()
	apiNameToScope := FrontendAPIMetricsScopes()
	s.assertAllAPIsHaveMetricsDefined(apiNames, apiNameToScope)
	s.Equal(len(apiNames), len(apiNameToScope))
}

func (s *apiMetricsSuite) TestMatchingAPIMetrics() {
	apiNames := matchingAPIMetricsNames()
	apiNameToScope := MatchingAPIMetricsScopes()
	s.assertAllAPIsHaveMetricsDefined(apiNames, apiNameToScope)
	s.Equal(len(apiNames), len(apiNameToScope))
}

func (s *apiMetricsSuite) TestHistoryAPIMetrics() {
	apiNames := historyAPIMetricsNames()
	apiNameToScope := HistoryAPIMetricsScopes()
	s.assertAllAPIsHaveMetricsDefined(apiNames, apiNameToScope)
	s.Equal(len(apiNames), len(apiNameToScope))
}
