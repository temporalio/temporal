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

package quotas

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	priorityReservationSuite struct {
		suite.Suite
		*require.Assertions

		controller          *gomock.Controller
		decidingReservation *MockReservation
		otherReservation    *MockReservation
	}
)

func TestPriorityReservationSuite(t *testing.T) {
	s := new(priorityReservationSuite)
	suite.Run(t, s)
}

func (s *priorityReservationSuite) SetupSuite() {

}

func (s *priorityReservationSuite) TearDownSuite() {

}

func (s *priorityReservationSuite) SetupTest() {
	s.Assertions = require.New(s.T())

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
	s.False(result)
}

func (s *priorityReservationSuite) Test_OK_OK() {
	s.decidingReservation.EXPECT().OK().Return(true)
	reservation := NewPriorityReservation(s.decidingReservation, []Reservation{s.otherReservation})

	result := reservation.OK()
	s.True(result)
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
	s.Equal(decidingReservationDelay, result)
}
