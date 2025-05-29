package quotas

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"
)

type (
	multiReservationSuite struct {
		suite.Suite
		*require.Assertions

		controller        *gomock.Controller
		firstReservation  *MockReservation
		secondReservation *MockReservation
	}
)

func TestMultiReservationSuite(t *testing.T) {
	s := new(multiReservationSuite)
	suite.Run(t, s)
}

func (s *multiReservationSuite) SetupSuite() {

}

func (s *multiReservationSuite) TearDownSuite() {

}

func (s *multiReservationSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.firstReservation = NewMockReservation(s.controller)
	s.secondReservation = NewMockReservation(s.controller)
}

func (s *multiReservationSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *multiReservationSuite) Test_NotOK_OK() {
	reservation := NewMultiReservation(false, nil)

	result := reservation.OK()
	s.False(result)
}

func (s *multiReservationSuite) Test_NotOK_CancelAt() {
	now := time.Now()
	reservation := NewMultiReservation(false, nil)

	reservation.CancelAt(now)
}

func (s *multiReservationSuite) Test_NotOK_DelayFrom() {
	now := time.Now()
	reservation := NewMultiReservation(false, nil)

	result := reservation.DelayFrom(now)
	s.Equal(InfDuration, result)
}

func (s *multiReservationSuite) Test_OK_OK() {
	reservation := NewMultiReservation(true, []Reservation{s.firstReservation, s.secondReservation})

	result := reservation.OK()
	s.True(result)
}

func (s *multiReservationSuite) Test_OK_CancelAt() {
	now := time.Now()
	s.firstReservation.EXPECT().CancelAt(now)
	s.secondReservation.EXPECT().CancelAt(now)
	reservation := NewMultiReservation(true, []Reservation{s.firstReservation, s.secondReservation})

	reservation.CancelAt(now)
}

func (s *multiReservationSuite) Test_OK_DelayFrom() {
	now := time.Now()
	firstReservationDelay := time.Second
	secondReservationDelay := time.Minute
	s.firstReservation.EXPECT().DelayFrom(now).Return(firstReservationDelay).AnyTimes()
	s.secondReservation.EXPECT().DelayFrom(now).Return(secondReservationDelay).AnyTimes()
	reservation := NewMultiReservation(true, []Reservation{s.firstReservation, s.secondReservation})

	result := reservation.DelayFrom(now)
	s.Equal(secondReservationDelay, result)
}
