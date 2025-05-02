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
		func(ctx context.Context, hostAddress string) (enumsspb.HealthState, error) {
			switch hostAddress {
			case "1", "3":
				return enumsspb.HEALTH_STATE_SERVING, nil
			case "2":
				return enumsspb.HEALTH_STATE_UNSPECIFIED, fmt.Errorf("test")
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
		membership.NewHostInfoFromAddress("4"),
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
