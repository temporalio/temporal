package quotas

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"
)

type (
	priorityReservationSuite struct {
		suite.Suite

		controller          *gomock.Controller
		decidingReservation *MockReservation
		otherReservation    *MockReservation
	}
)

func TestPriorityReservationSuite(t *testing.T) {
	s := new(priorityReservationSuite)
	suite.Run(t, s)
}

func (s *priorityReservationSuite) TearDownSuite() {

}

func (s *priorityReservationSuite) SetupTest() {

	s.controller = gomock.NewController(s.T())
	s.decidingReservation = NewMockReservation(s.controller)
	s.otherReservation = NewMockReservation(s.controller)
}

func (s *priorityReservationSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *priorityReservationSuite) Test_NotOK_OK() {
	s.decidingReservation.EXPECT().OK().Return(false)
	reservation := NewPriorityReservation(s.decidingReservation, []Reservation{s.otherReservation})

	result := reservation.OK()
	require.False(s.T(), result)
}

func (s *priorityReservationSuite) Test_OK_OK() {
	s.decidingReservation.EXPECT().OK().Return(true)
	reservation := NewPriorityReservation(s.decidingReservation, []Reservation{s.otherReservation})

	result := reservation.OK()
	require.True(s.T(), result)
}

func (s *priorityReservationSuite) Test_CancelAt() {
	now := time.Now()
	s.decidingReservation.EXPECT().CancelAt(now)
	s.otherReservation.EXPECT().CancelAt(now)
	reservation := NewPriorityReservation(s.decidingReservation, []Reservation{s.otherReservation})

	reservation.CancelAt(now)
}

func (s *priorityReservationSuite) Test_DelayFrom() {
	now := time.Now()
	decidingReservationDelay := time.Second
	otherReservationDelay := time.Minute
	s.decidingReservation.EXPECT().DelayFrom(now).Return(decidingReservationDelay)
	s.otherReservation.EXPECT().DelayFrom(now).Return(otherReservationDelay).Times(0)
	reservation := NewPriorityReservation(s.decidingReservation, []Reservation{s.otherReservation})

	result := reservation.DelayFrom(now)
	require.Equal(s.T(), decidingReservationDelay, result)
}
