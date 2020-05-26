// The MIT License (MIT)
//
// Copyright (c) 2017-2020 Uber Technologies Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package invariants

import (
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/uber/cadence/service/worker/scanner/executions/common"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type InvariantManagerSuite struct {
	*require.Assertions
	suite.Suite
}

func TestInvariantManagerSuite(t *testing.T) {
	suite.Run(t, new(InvariantManagerSuite))
}

func (s *InvariantManagerSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *InvariantManagerSuite) TestRunChecks() {
	testCases := []struct {
		checkResults []common.CheckResult
		expected     common.ManagerCheckResult
	}{
		{
			checkResults: nil,
			expected: common.ManagerCheckResult{
				CheckResultType: common.CheckResultTypeHealthy,
				CheckResults:    nil,
			},
		},
		{
			checkResults: []common.CheckResult{
				{
					CheckResultType: common.CheckResultTypeHealthy,
					Info:            "invariant 1 info",
					InfoDetails:     "invariant 1 info details",
				},
				{
					CheckResultType: common.CheckResultTypeFailed,
					Info:            "invariant 2 info",
					InfoDetails:     "invariant 2 info details",
				},
			},
			expected: common.ManagerCheckResult{
				CheckResultType: common.CheckResultTypeFailed,
				CheckResults: []common.CheckResult{
					{
						CheckResultType: common.CheckResultTypeHealthy,
						Info:            "invariant 1 info",
						InfoDetails:     "invariant 1 info details",
					},
					{
						CheckResultType: common.CheckResultTypeFailed,
						Info:            "invariant 2 info",
						InfoDetails:     "invariant 2 info details",
					},
				},
			},
		},
		{
			checkResults: []common.CheckResult{
				{
					CheckResultType: common.CheckResultTypeHealthy,
					Info:            "invariant 1 info",
					InfoDetails:     "invariant 1 info details",
				},
				{
					CheckResultType: common.CheckResultTypeCorrupted,
					Info:            "invariant 2 info",
					InfoDetails:     "invariant 2 info details",
				},
			},
			expected: common.ManagerCheckResult{
				CheckResultType: common.CheckResultTypeCorrupted,
				CheckResults: []common.CheckResult{
					{
						CheckResultType: common.CheckResultTypeHealthy,
						Info:            "invariant 1 info",
						InfoDetails:     "invariant 1 info details",
					},
					{
						CheckResultType: common.CheckResultTypeCorrupted,
						Info:            "invariant 2 info",
						InfoDetails:     "invariant 2 info details",
					},
				},
			},
		},
		{
			checkResults: []common.CheckResult{
				{
					CheckResultType: common.CheckResultTypeHealthy,
					Info:            "invariant 1 info",
					InfoDetails:     "invariant 1 info details",
				},
				{
					CheckResultType: common.CheckResultTypeHealthy,
					Info:            "invariant 2 info",
					InfoDetails:     "invariant 2 info details",
				},
			},
			expected: common.ManagerCheckResult{
				CheckResultType: common.CheckResultTypeHealthy,
				CheckResults: []common.CheckResult{
					{
						CheckResultType: common.CheckResultTypeHealthy,
						Info:            "invariant 1 info",
						InfoDetails:     "invariant 1 info details",
					},
					{
						CheckResultType: common.CheckResultTypeHealthy,
						Info:            "invariant 2 info",
						InfoDetails:     "invariant 2 info details",
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		invariants := make([]common.Invariant, len(tc.checkResults), len(tc.checkResults))
		for i := 0; i < len(tc.checkResults); i++ {
			invariant := &common.MockInvariant{}
			invariant.On("Check", mock.Anything, mock.Anything).Return(tc.checkResults[i])
			invariants[i] = invariant
		}
		manager := &invariantManager{
			invariants: invariants,
		}
		s.Equal(tc.expected, manager.RunChecks(common.Execution{}))
	}
}

func (s *InvariantManagerSuite) TestRunFixes() {
	testCases := []struct {
		fixResults []common.FixResult
		expected   common.ManagerFixResult
	}{
		{
			fixResults: nil,
			expected: common.ManagerFixResult{
				FixResultType: common.FixResultTypeSkipped,
				FixResults:    nil,
			},
		},
		{
			fixResults: []common.FixResult{
				{
					FixResultType: common.FixResultTypeFixed,
					CheckResult: common.CheckResult{
						CheckResultType: common.CheckResultTypeCorrupted,
						Info:            "invariant 1 check info",
						InfoDetails:     "invariant 1 check info details",
					},
					Info:        "invariant 1 info",
					InfoDetails: "invariant 1 info details",
				},
				{
					FixResultType: common.FixResultTypeFailed,
					CheckResult: common.CheckResult{
						CheckResultType: common.CheckResultTypeCorrupted,
						Info:            "invariant 2 check info",
						InfoDetails:     "invariant 2 check info details",
					},
					Info:        "invariant 2 info",
					InfoDetails: "invariant 2 info details",
				},
			},
			expected: common.ManagerFixResult{
				FixResultType: common.FixResultTypeFailed,
				FixResults: []common.FixResult{
					{
						FixResultType: common.FixResultTypeFixed,
						CheckResult: common.CheckResult{
							CheckResultType: common.CheckResultTypeCorrupted,
							Info:            "invariant 1 check info",
							InfoDetails:     "invariant 1 check info details",
						},
						Info:        "invariant 1 info",
						InfoDetails: "invariant 1 info details",
					},
					{
						FixResultType: common.FixResultTypeFailed,
						CheckResult: common.CheckResult{
							CheckResultType: common.CheckResultTypeCorrupted,
							Info:            "invariant 2 check info",
							InfoDetails:     "invariant 2 check info details",
						},
						Info:        "invariant 2 info",
						InfoDetails: "invariant 2 info details",
					},
				},
			},
		},
		{
			fixResults: []common.FixResult{
				{
					FixResultType: common.FixResultTypeSkipped,
					CheckResult: common.CheckResult{
						CheckResultType: common.CheckResultTypeHealthy,
						Info:            "invariant 1 check info",
						InfoDetails:     "invariant 1 check info details",
					},
					Info:        "invariant 1 info",
					InfoDetails: "invariant 1 info details",
				},
				{
					FixResultType: common.FixResultTypeSkipped,
					CheckResult: common.CheckResult{
						CheckResultType: common.CheckResultTypeHealthy,
						Info:            "invariant 2 check info",
						InfoDetails:     "invariant 2 check info details",
					},
					Info:        "invariant 2 info",
					InfoDetails: "invariant 2 info details",
				},
			},
			expected: common.ManagerFixResult{
				FixResultType: common.FixResultTypeSkipped,
				FixResults: []common.FixResult{
					{
						FixResultType: common.FixResultTypeSkipped,
						CheckResult: common.CheckResult{
							CheckResultType: common.CheckResultTypeHealthy,
							Info:            "invariant 1 check info",
							InfoDetails:     "invariant 1 check info details",
						},
						Info:        "invariant 1 info",
						InfoDetails: "invariant 1 info details",
					},
					{
						FixResultType: common.FixResultTypeSkipped,
						CheckResult: common.CheckResult{
							CheckResultType: common.CheckResultTypeHealthy,
							Info:            "invariant 2 check info",
							InfoDetails:     "invariant 2 check info details",
						},
						Info:        "invariant 2 info",
						InfoDetails: "invariant 2 info details",
					},
				},
			},
		},
		{
			fixResults: []common.FixResult{
				{
					FixResultType: common.FixResultTypeSkipped,
					CheckResult: common.CheckResult{
						CheckResultType: common.CheckResultTypeHealthy,
						Info:            "invariant 1 check info",
						InfoDetails:     "invariant 1 check info details",
					},
					Info:        "invariant 1 info",
					InfoDetails: "invariant 1 info details",
				},
				{
					FixResultType: common.FixResultTypeFixed,
					CheckResult: common.CheckResult{
						CheckResultType: common.CheckResultTypeCorrupted,
						Info:            "invariant 2 check info",
						InfoDetails:     "invariant 2 check info details",
					},
					Info:        "invariant 2 info",
					InfoDetails: "invariant 2 info details",
				},
				{
					FixResultType: common.FixResultTypeSkipped,
					CheckResult: common.CheckResult{
						CheckResultType: common.CheckResultTypeHealthy,
						Info:            "invariant 3 check info",
						InfoDetails:     "invariant 3 check info details",
					},
					Info:        "invariant 3 info",
					InfoDetails: "invariant 3 info details",
				},
			},
			expected: common.ManagerFixResult{
				FixResultType: common.FixResultTypeFixed,
				FixResults: []common.FixResult{
					{
						FixResultType: common.FixResultTypeSkipped,
						CheckResult: common.CheckResult{
							CheckResultType: common.CheckResultTypeHealthy,
							Info:            "invariant 1 check info",
							InfoDetails:     "invariant 1 check info details",
						},
						Info:        "invariant 1 info",
						InfoDetails: "invariant 1 info details",
					},
					{
						FixResultType: common.FixResultTypeFixed,
						CheckResult: common.CheckResult{
							CheckResultType: common.CheckResultTypeCorrupted,
							Info:            "invariant 2 check info",
							InfoDetails:     "invariant 2 check info details",
						},
						Info:        "invariant 2 info",
						InfoDetails: "invariant 2 info details",
					},
					{
						FixResultType: common.FixResultTypeSkipped,
						CheckResult: common.CheckResult{
							CheckResultType: common.CheckResultTypeHealthy,
							Info:            "invariant 3 check info",
							InfoDetails:     "invariant 3 check info details",
						},
						Info:        "invariant 3 info",
						InfoDetails: "invariant 3 info details",
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		invariants := make([]common.Invariant, len(tc.fixResults), len(tc.fixResults))
		for i := 0; i < len(tc.fixResults); i++ {
			invariant := &common.MockInvariant{}
			invariant.On("Fix", mock.Anything, mock.Anything).Return(tc.fixResults[i])
			invariants[i] = invariant
		}
		manager := &invariantManager{
			invariants: invariants,
		}
		s.Equal(tc.expected, manager.RunFixes(common.Execution{}))
	}
}
