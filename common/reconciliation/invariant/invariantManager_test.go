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

package invariant

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/common/reconciliation/entity"
)

type InvariantManagerSuite struct {
	*require.Assertions
	suite.Suite
	controller *gomock.Controller
}

func TestInvariantManagerSuite(t *testing.T) {
	suite.Run(t, new(InvariantManagerSuite))
}

func (s *InvariantManagerSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
}

func (s *InvariantManagerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *InvariantManagerSuite) TestRunChecks() {
	testCases := []struct {
		checkResults []CheckResult
		expected     ManagerCheckResult
	}{
		{
			checkResults: nil,
			expected: ManagerCheckResult{
				CheckResultType: CheckResultTypeHealthy,
				CheckResults:    nil,
			},
		},
		{
			checkResults: []CheckResult{
				{
					CheckResultType: CheckResultTypeHealthy,
					InvariantName:   Name("first"),
					Info:            "invariant 1 info",
					InfoDetails:     "invariant 1 info details",
				},
				{
					CheckResultType: CheckResultTypeFailed,
					InvariantName:   Name("second"),
					Info:            "invariant 2 info",
					InfoDetails:     "invariant 2 info details",
				},
			},
			expected: ManagerCheckResult{
				CheckResultType:          CheckResultTypeFailed,
				DeterminingInvariantType: NamePtr("second"),
				CheckResults: []CheckResult{
					{
						CheckResultType: CheckResultTypeHealthy,
						InvariantName:   Name("first"),
						Info:            "invariant 1 info",
						InfoDetails:     "invariant 1 info details",
					},
					{
						CheckResultType: CheckResultTypeFailed,
						InvariantName:   Name("second"),
						Info:            "invariant 2 info",
						InfoDetails:     "invariant 2 info details",
					},
				},
			},
		},
		{
			checkResults: []CheckResult{
				{
					CheckResultType: CheckResultTypeHealthy,
					InvariantName:   Name("first"),
					Info:            "invariant 1 info",
					InfoDetails:     "invariant 1 info details",
				},
				{
					CheckResultType: CheckResultTypeCorrupted,
					InvariantName:   Name("second"),
					Info:            "invariant 2 info",
					InfoDetails:     "invariant 2 info details",
				},
			},
			expected: ManagerCheckResult{
				CheckResultType:          CheckResultTypeCorrupted,
				DeterminingInvariantType: NamePtr("second"),
				CheckResults: []CheckResult{
					{
						CheckResultType: CheckResultTypeHealthy,
						InvariantName:   Name("first"),
						Info:            "invariant 1 info",
						InfoDetails:     "invariant 1 info details",
					},
					{
						CheckResultType: CheckResultTypeCorrupted,
						InvariantName:   Name("second"),
						Info:            "invariant 2 info",
						InfoDetails:     "invariant 2 info details",
					},
				},
			},
		},
		{
			checkResults: []CheckResult{
				{
					CheckResultType: CheckResultTypeHealthy,
					InvariantName:   Name("first"),
					Info:            "invariant 1 info",
					InfoDetails:     "invariant 1 info details",
				},
				{
					CheckResultType: CheckResultTypeHealthy,
					InvariantName:   Name("second"),
					Info:            "invariant 2 info",
					InfoDetails:     "invariant 2 info details",
				},
			},
			expected: ManagerCheckResult{
				CheckResultType:          CheckResultTypeHealthy,
				DeterminingInvariantType: nil,
				CheckResults: []CheckResult{
					{
						CheckResultType: CheckResultTypeHealthy,
						InvariantName:   Name("first"),
						Info:            "invariant 1 info",
						InfoDetails:     "invariant 1 info details",
					},
					{
						CheckResultType: CheckResultTypeHealthy,
						InvariantName:   Name("second"),
						Info:            "invariant 2 info",
						InfoDetails:     "invariant 2 info details",
					},
				},
			},
		},
		{
			checkResults: []CheckResult{
				{
					CheckResultType: CheckResultTypeHealthy,
					InvariantName:   Name("first"),
					Info:            "invariant 1 info",
					InfoDetails:     "invariant 1 info details",
				},
				{
					CheckResultType: CheckResultTypeCorrupted,
					InvariantName:   Name("second"),
					Info:            "invariant 2 info",
					InfoDetails:     "invariant 2 info details",
				},
				{
					CheckResultType: CheckResultTypeFailed,
					InvariantName:   Name("third"),
					Info:            "invariant 3 info",
					InfoDetails:     "invariant 3 info details",
				},
				{
					CheckResultType: CheckResultTypeHealthy,
					InvariantName:   Name("forth"),
					Info:            "invariant 4 info",
					InfoDetails:     "invariant 4 info details",
				},
			},
			expected: ManagerCheckResult{
				CheckResultType:          CheckResultTypeFailed,
				DeterminingInvariantType: NamePtr("third"),
				CheckResults: []CheckResult{
					{
						CheckResultType: CheckResultTypeHealthy,
						InvariantName:   Name("first"),
						Info:            "invariant 1 info",
						InfoDetails:     "invariant 1 info details",
					},
					{
						CheckResultType: CheckResultTypeCorrupted,
						InvariantName:   Name("second"),
						Info:            "invariant 2 info",
						InfoDetails:     "invariant 2 info details",
					},
					{
						CheckResultType: CheckResultTypeFailed,
						InvariantName:   Name("third"),
						Info:            "invariant 3 info",
						InfoDetails:     "invariant 3 info details",
					},
					{
						CheckResultType: CheckResultTypeHealthy,
						InvariantName:   Name("forth"),
						Info:            "invariant 4 info",
						InfoDetails:     "invariant 4 info details",
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		invariants := make([]Invariant, len(tc.checkResults), len(tc.checkResults))
		for i := 0; i < len(tc.checkResults); i++ {
			mockInvariant := NewMockInvariant(s.controller)
			mockInvariant.EXPECT().Check(gomock.Any()).Return(tc.checkResults[i])
			invariants[i] = mockInvariant
		}
		manager := &invariantManager{
			invariants: invariants,
		}
		s.Equal(tc.expected, manager.RunChecks(entity.Execution{}))
	}
}

func (s *InvariantManagerSuite) TestRunFixes() {
	testCases := []struct {
		fixResults []FixResult
		expected   ManagerFixResult
	}{
		{
			fixResults: nil,
			expected: ManagerFixResult{
				FixResultType:            FixResultTypeSkipped,
				DeterminingInvariantType: nil,
				FixResults:               nil,
			},
		},
		{
			fixResults: []FixResult{
				{
					FixResultType: FixResultTypeFixed,
					InvariantType: Name("first"),
					CheckResult: CheckResult{
						CheckResultType: CheckResultTypeCorrupted,
						Info:            "invariant 1 check info",
						InfoDetails:     "invariant 1 check info details",
					},
					Info:        "invariant 1 info",
					InfoDetails: "invariant 1 info details",
				},
				{
					FixResultType: FixResultTypeFailed,
					InvariantType: Name("second"),
					CheckResult: CheckResult{
						CheckResultType: CheckResultTypeCorrupted,
						Info:            "invariant 2 check info",
						InfoDetails:     "invariant 2 check info details",
					},
					Info:        "invariant 2 info",
					InfoDetails: "invariant 2 info details",
				},
			},
			expected: ManagerFixResult{
				FixResultType:            FixResultTypeFailed,
				DeterminingInvariantType: NamePtr("second"),
				FixResults: []FixResult{
					{
						FixResultType: FixResultTypeFixed,
						InvariantType: Name("first"),
						CheckResult: CheckResult{
							CheckResultType: CheckResultTypeCorrupted,
							Info:            "invariant 1 check info",
							InfoDetails:     "invariant 1 check info details",
						},
						Info:        "invariant 1 info",
						InfoDetails: "invariant 1 info details",
					},
					{
						FixResultType: FixResultTypeFailed,
						InvariantType: Name("second"),
						CheckResult: CheckResult{
							CheckResultType: CheckResultTypeCorrupted,
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
			fixResults: []FixResult{
				{
					FixResultType: FixResultTypeSkipped,
					InvariantType: Name("first"),
					CheckResult: CheckResult{
						CheckResultType: CheckResultTypeHealthy,
						Info:            "invariant 1 check info",
						InfoDetails:     "invariant 1 check info details",
					},
					Info:        "invariant 1 info",
					InfoDetails: "invariant 1 info details",
				},
				{
					FixResultType: FixResultTypeSkipped,
					InvariantType: Name("second"),
					CheckResult: CheckResult{
						CheckResultType: CheckResultTypeHealthy,
						Info:            "invariant 2 check info",
						InfoDetails:     "invariant 2 check info details",
					},
					Info:        "invariant 2 info",
					InfoDetails: "invariant 2 info details",
				},
			},
			expected: ManagerFixResult{
				FixResultType:            FixResultTypeSkipped,
				DeterminingInvariantType: nil,
				FixResults: []FixResult{
					{
						FixResultType: FixResultTypeSkipped,
						InvariantType: Name("first"),
						CheckResult: CheckResult{
							CheckResultType: CheckResultTypeHealthy,
							Info:            "invariant 1 check info",
							InfoDetails:     "invariant 1 check info details",
						},
						Info:        "invariant 1 info",
						InfoDetails: "invariant 1 info details",
					},
					{
						FixResultType: FixResultTypeSkipped,
						InvariantType: Name("second"),
						CheckResult: CheckResult{
							CheckResultType: CheckResultTypeHealthy,
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
			fixResults: []FixResult{
				{
					FixResultType: FixResultTypeSkipped,
					InvariantType: Name("first"),
					CheckResult: CheckResult{
						CheckResultType: CheckResultTypeHealthy,
						Info:            "invariant 1 check info",
						InfoDetails:     "invariant 1 check info details",
					},
					Info:        "invariant 1 info",
					InfoDetails: "invariant 1 info details",
				},
				{
					FixResultType: FixResultTypeFixed,
					InvariantType: Name("second"),
					CheckResult: CheckResult{
						CheckResultType: CheckResultTypeCorrupted,
						Info:            "invariant 2 check info",
						InfoDetails:     "invariant 2 check info details",
					},
					Info:        "invariant 2 info",
					InfoDetails: "invariant 2 info details",
				},
				{
					FixResultType: FixResultTypeSkipped,
					InvariantType: Name("third"),
					CheckResult: CheckResult{
						CheckResultType: CheckResultTypeHealthy,
						Info:            "invariant 3 check info",
						InfoDetails:     "invariant 3 check info details",
					},
					Info:        "invariant 3 info",
					InfoDetails: "invariant 3 info details",
				},
			},
			expected: ManagerFixResult{
				FixResultType:            FixResultTypeFixed,
				DeterminingInvariantType: NamePtr("second"),
				FixResults: []FixResult{
					{
						FixResultType: FixResultTypeSkipped,
						InvariantType: Name("first"),
						CheckResult: CheckResult{
							CheckResultType: CheckResultTypeHealthy,
							Info:            "invariant 1 check info",
							InfoDetails:     "invariant 1 check info details",
						},
						Info:        "invariant 1 info",
						InfoDetails: "invariant 1 info details",
					},
					{
						FixResultType: FixResultTypeFixed,
						InvariantType: Name("second"),
						CheckResult: CheckResult{
							CheckResultType: CheckResultTypeCorrupted,
							Info:            "invariant 2 check info",
							InfoDetails:     "invariant 2 check info details",
						},
						Info:        "invariant 2 info",
						InfoDetails: "invariant 2 info details",
					},
					{
						FixResultType: FixResultTypeSkipped,
						InvariantType: Name("third"),
						CheckResult: CheckResult{
							CheckResultType: CheckResultTypeHealthy,
							Info:            "invariant 3 check info",
							InfoDetails:     "invariant 3 check info details",
						},
						Info:        "invariant 3 info",
						InfoDetails: "invariant 3 info details",
					},
				},
			},
		},
		{
			fixResults: []FixResult{
				{
					FixResultType: FixResultTypeSkipped,
					InvariantType: Name("first"),
					CheckResult: CheckResult{
						CheckResultType: CheckResultTypeHealthy,
						Info:            "invariant 1 check info",
						InfoDetails:     "invariant 1 check info details",
					},
					Info:        "invariant 1 info",
					InfoDetails: "invariant 1 info details",
				},
				{
					FixResultType: FixResultTypeFixed,
					InvariantType: Name("second"),
					CheckResult: CheckResult{
						CheckResultType: CheckResultTypeCorrupted,
						Info:            "invariant 2 check info",
						InfoDetails:     "invariant 2 check info details",
					},
					Info:        "invariant 2 info",
					InfoDetails: "invariant 2 info details",
				},
				{
					FixResultType: FixResultTypeSkipped,
					InvariantType: Name("third"),
					CheckResult: CheckResult{
						CheckResultType: CheckResultTypeHealthy,
						Info:            "invariant 3 check info",
						InfoDetails:     "invariant 3 check info details",
					},
					Info:        "invariant 3 info",
					InfoDetails: "invariant 3 info details",
				},
				{
					FixResultType: FixResultTypeFailed,
					InvariantType: Name("forth"),
					CheckResult: CheckResult{
						CheckResultType: CheckResultTypeCorrupted,
						Info:            "invariant 4 check info",
						InfoDetails:     "invariant 2 check info details",
					},
					Info:        "invariant 4 info",
					InfoDetails: "invariant 4 info details",
				},
			},
			expected: ManagerFixResult{
				FixResultType:            FixResultTypeFailed,
				DeterminingInvariantType: NamePtr("forth"),
				FixResults: []FixResult{
					{
						FixResultType: FixResultTypeSkipped,
						InvariantType: Name("first"),
						CheckResult: CheckResult{
							CheckResultType: CheckResultTypeHealthy,
							Info:            "invariant 1 check info",
							InfoDetails:     "invariant 1 check info details",
						},
						Info:        "invariant 1 info",
						InfoDetails: "invariant 1 info details",
					},
					{
						FixResultType: FixResultTypeFixed,
						InvariantType: Name("second"),
						CheckResult: CheckResult{
							CheckResultType: CheckResultTypeCorrupted,
							Info:            "invariant 2 check info",
							InfoDetails:     "invariant 2 check info details",
						},
						Info:        "invariant 2 info",
						InfoDetails: "invariant 2 info details",
					},
					{
						FixResultType: FixResultTypeSkipped,
						InvariantType: Name("third"),
						CheckResult: CheckResult{
							CheckResultType: CheckResultTypeHealthy,
							Info:            "invariant 3 check info",
							InfoDetails:     "invariant 3 check info details",
						},
						Info:        "invariant 3 info",
						InfoDetails: "invariant 3 info details",
					},
					{
						FixResultType: FixResultTypeFailed,
						InvariantType: Name("forth"),
						CheckResult: CheckResult{
							CheckResultType: CheckResultTypeCorrupted,
							Info:            "invariant 4 check info",
							InfoDetails:     "invariant 2 check info details",
						},
						Info:        "invariant 4 info",
						InfoDetails: "invariant 4 info details",
					},
				},
			},
		},
		{
			fixResults: []FixResult{
				{
					FixResultType: FixResultTypeSkipped,
					InvariantType: Name("first"),
					CheckResult: CheckResult{
						CheckResultType: CheckResultTypeHealthy,
						Info:            "invariant 1 check info",
						InfoDetails:     "invariant 1 check info details",
					},
					Info:        "invariant 1 info",
					InfoDetails: "invariant 1 info details",
				},
				{
					FixResultType: FixResultTypeFailed,
					InvariantType: Name("second"),
					CheckResult: CheckResult{
						CheckResultType: CheckResultTypeCorrupted,
						Info:            "invariant 4 check info",
						InfoDetails:     "invariant 2 check info details",
					},
					Info:        "invariant 4 info",
					InfoDetails: "invariant 4 info details",
				},
				{
					FixResultType: FixResultTypeFixed,
					InvariantType: Name("third"),
					CheckResult: CheckResult{
						CheckResultType: CheckResultTypeCorrupted,
						Info:            "invariant 2 check info",
						InfoDetails:     "invariant 2 check info details",
					},
					Info:        "invariant 2 info",
					InfoDetails: "invariant 2 info details",
				},
				{
					FixResultType: FixResultTypeSkipped,
					InvariantType: Name("forth"),
					CheckResult: CheckResult{
						CheckResultType: CheckResultTypeHealthy,
						Info:            "invariant 3 check info",
						InfoDetails:     "invariant 3 check info details",
					},
					Info:        "invariant 3 info",
					InfoDetails: "invariant 3 info details",
				},
			},
			expected: ManagerFixResult{
				FixResultType:            FixResultTypeFailed,
				DeterminingInvariantType: NamePtr("second"),
				FixResults: []FixResult{
					{
						FixResultType: FixResultTypeSkipped,
						InvariantType: Name("first"),
						CheckResult: CheckResult{
							CheckResultType: CheckResultTypeHealthy,
							Info:            "invariant 1 check info",
							InfoDetails:     "invariant 1 check info details",
						},
						Info:        "invariant 1 info",
						InfoDetails: "invariant 1 info details",
					},
					{
						FixResultType: FixResultTypeFailed,
						InvariantType: Name("second"),
						CheckResult: CheckResult{
							CheckResultType: CheckResultTypeCorrupted,
							Info:            "invariant 4 check info",
							InfoDetails:     "invariant 2 check info details",
						},
						Info:        "invariant 4 info",
						InfoDetails: "invariant 4 info details",
					},
					{
						FixResultType: FixResultTypeFixed,
						InvariantType: Name("third"),
						CheckResult: CheckResult{
							CheckResultType: CheckResultTypeCorrupted,
							Info:            "invariant 2 check info",
							InfoDetails:     "invariant 2 check info details",
						},
						Info:        "invariant 2 info",
						InfoDetails: "invariant 2 info details",
					},
					{
						FixResultType: FixResultTypeSkipped,
						InvariantType: Name("forth"),
						CheckResult: CheckResult{
							CheckResultType: CheckResultTypeHealthy,
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
		invariants := make([]Invariant, len(tc.fixResults), len(tc.fixResults))
		for i := 0; i < len(tc.fixResults); i++ {
			mockInvariant := NewMockInvariant(s.controller)
			mockInvariant.EXPECT().Fix(gomock.Any()).Return(tc.fixResults[i])
			invariants[i] = mockInvariant
		}
		manager := &invariantManager{
			invariants: invariants,
		}
		s.Equal(tc.expected, manager.RunFixes(entity.Execution{}))
	}
}
