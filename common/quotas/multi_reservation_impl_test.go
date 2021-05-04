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
