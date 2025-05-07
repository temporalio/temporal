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

package frontend

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/primitives"
	"go.uber.org/mock/gomock"
)

type (
	healthCheckerSuite struct {
		suite.Suite
		controller *gomock.Controller

		membershipMonitor *membership.MockMonitor
		resolver          *membership.MockServiceResolver

		checker *healthCheckerImpl
	}
)

func TestHealthCheckerSuite(t *testing.T) {
	s := new(healthCheckerSuite)
	suite.Run(t, s)
}

func (s *healthCheckerSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.membershipMonitor = membership.NewMockMonitor(s.controller)
	s.resolver = membership.NewMockServiceResolver(s.controller)
	s.membershipMonitor.EXPECT().GetResolver(gomock.Any()).Return(s.resolver, nil).AnyTimes()

	checker := NewHealthChecker(
		primitives.HistoryService,
		s.membershipMonitor,
		func() float64 {
			return 0.25
		},
		func() float64 {
			return 0.15
		},
		func(ctx context.Context, hostAddress string) (enumsspb.HealthState, error) {
			switch hostAddress {
			case "1", "3":
				return enumsspb.HEALTH_STATE_SERVING, nil
			case "2":
				return enumsspb.HEALTH_STATE_UNSPECIFIED, fmt.Errorf("test")
			case "4":
				return enumsspb.HEALTH_STATE_DECLINED_SERVING, nil
			default:
				return enumsspb.HEALTH_STATE_NOT_SERVING, nil
			}
		},
		log.NewNoopLogger(),
	)
	healthChecker, ok := checker.(*healthCheckerImpl)
	if !ok {
		s.Fail("The constructor did not return correct type")
	}
	s.checker = healthChecker
}

func (s *healthCheckerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *healthCheckerSuite) Test_Check_Serving() {
	s.resolver.EXPECT().AvailableMembers().Return([]membership.HostInfo{
		membership.NewHostInfoFromAddress("1"),
		membership.NewHostInfoFromAddress("2"),
		membership.NewHostInfoFromAddress("3"),
		membership.NewHostInfoFromAddress("1"),
	})

	state, err := s.checker.Check(context.Background())
	s.NoError(err)
	s.Equal(enumsspb.HEALTH_STATE_SERVING, state)
}

func (s *healthCheckerSuite) Test_Check_Not_Serving() {
	s.resolver.EXPECT().AvailableMembers().Return([]membership.HostInfo{
		membership.NewHostInfoFromAddress("1"),
		membership.NewHostInfoFromAddress("2"),
		membership.NewHostInfoFromAddress("3"),
		membership.NewHostInfoFromAddress("4"),
		membership.NewHostInfoFromAddress("5"),
	})

	state, err := s.checker.Check(context.Background())
	s.NoError(err)
	s.Equal(enumsspb.HEALTH_STATE_NOT_SERVING, state)
}

func (s *healthCheckerSuite) Test_Check_Declined_Serving() {
	s.resolver.EXPECT().AvailableMembers().Return([]membership.HostInfo{
		membership.NewHostInfoFromAddress("1"),
		membership.NewHostInfoFromAddress("2"),
		membership.NewHostInfoFromAddress("4"),
		membership.NewHostInfoFromAddress("4"),
		membership.NewHostInfoFromAddress("4"),
		membership.NewHostInfoFromAddress("4"),
		membership.NewHostInfoFromAddress("7"),
	})

	state, err := s.checker.Check(context.Background())
	s.NoError(err)
	s.Equal(enumsspb.HEALTH_STATE_DECLINED_SERVING, state)
}

func (s *healthCheckerSuite) Test_GetProportionOfNotReadyHosts() {
	testCases := []struct {
		name                             string
		proportionOfDeclinedServingHosts float64
		totalHosts                       int
		expectedProportion               float64
	}{
		{
			name:                             "zero proportion",
			proportionOfDeclinedServingHosts: 0.0,
			totalHosts:                       10,
			expectedProportion:               0.0,
		},
		{
			name:                             "small proportion with few hosts",
			proportionOfDeclinedServingHosts: 0.1,
			totalHosts:                       10,
			expectedProportion:               0.2, // 2/10 = 0.2 since numHostsToFail < 2
		},
		{
			name:                             "small proportion with many hosts",
			proportionOfDeclinedServingHosts: 0.1,
			totalHosts:                       100,
			expectedProportion:               0.1, // 10 hosts > 2, so use original proportion
		},
		{
			name:                             "large proportion",
			proportionOfDeclinedServingHosts: 0.8,
			totalHosts:                       10,
			expectedProportion:               0.8, // 8 hosts > 2, so use original proportion
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			proportion := getProportionOfDeclinedServiceHosts(tc.proportionOfDeclinedServingHosts, tc.totalHosts)
			s.Equal(tc.expectedProportion, proportion)
		})
	}
}
