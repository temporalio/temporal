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

package quotas_test

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/quotas"
)

type (
	priorityStageRateLimiterSuite struct {
		suite.Suite
		*require.Assertions

		controller              *gomock.Controller
		highPriorityRateLimiter *quotas.MockRateLimiter
		lowPriorityRateLimiter  *quotas.MockRateLimiter
		highPriorityReservation *quotas.MockReservation
		lowPriorityReservation  *quotas.MockReservation
		highPriorityAPIName     string
		lowPriorityAPIName      string

		rateLimiter *quotas.PriorityRateLimiterImpl
		clock       clockwork.FakeClock
	}
)

func TestPriorityStageRateLimiterSuite(t *testing.T) {
	s := new(priorityStageRateLimiterSuite)
	suite.Run(t, s)
}

func (s *priorityStageRateLimiterSuite) SetupSuite() {

}

func (s *priorityStageRateLimiterSuite) TearDownSuite() {

}

func (s *priorityStageRateLimiterSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.highPriorityRateLimiter = quotas.NewMockRateLimiter(s.controller)
	s.lowPriorityRateLimiter = quotas.NewMockRateLimiter(s.controller)
	s.highPriorityReservation = quotas.NewMockReservation(s.controller)
	s.lowPriorityReservation = quotas.NewMockReservation(s.controller)

	s.highPriorityAPIName = "high-priority"
	s.lowPriorityAPIName = "low-priority"
	apiToPriority := map[string]int{
		s.highPriorityAPIName: 0,
		s.lowPriorityAPIName:  2,
	}
	priorityToRateLimiters := map[int]quotas.RequestRateLimiter{
		0: quotas.NewRequestRateLimiterAdapter(s.highPriorityRateLimiter),
		2: quotas.NewRequestRateLimiterAdapter(s.lowPriorityRateLimiter),
	}
	s.clock = clockwork.NewFakeClock()
	s.rateLimiter = quotas.NewPriorityRateLimiter(func(req quotas.Request) int {
		return apiToPriority[req.API]
	}, priorityToRateLimiters, s.clock)

}

func (s *priorityStageRateLimiterSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *priorityStageRateLimiterSuite) TestAllow_HighPriority_Allow() {
	now := time.Now()
	token := 1
	req := quotas.Request{
		API:    s.highPriorityAPIName,
		Token:  token,
		Caller: "",
	}

	s.highPriorityRateLimiter.EXPECT().AllowN(now, token).Return(true)
	s.lowPriorityRateLimiter.EXPECT().ReserveN(now, token).Return(s.lowPriorityReservation)

	allow := s.rateLimiter.Allow(now, req)
	s.True(allow)
}

func (s *priorityStageRateLimiterSuite) TestAllow_HighPriority_Disallow() {
	now := time.Now()
	token := 1
	req := quotas.Request{
		API:    s.highPriorityAPIName,
		Token:  token,
		Caller: "",
	}

	s.highPriorityRateLimiter.EXPECT().AllowN(now, token).Return(false)

	allow := s.rateLimiter.Allow(now, req)
	s.False(allow)
}

func (s *priorityStageRateLimiterSuite) TestAllow_LowPriority_Allow() {
	now := time.Now()
	token := 1
	req := quotas.Request{
		API:    s.lowPriorityAPIName,
		Token:  token,
		Caller: "",
	}

	s.lowPriorityRateLimiter.EXPECT().AllowN(now, token).Return(true)

	allow := s.rateLimiter.Allow(now, req)
	s.True(allow)
}

func (s *priorityStageRateLimiterSuite) TestAllow_LowPriority_Disallow() {
	now := time.Now()
	token := 1
	req := quotas.Request{
		API:    s.lowPriorityAPIName,
		Token:  token,
		Caller: "",
	}

	s.lowPriorityRateLimiter.EXPECT().AllowN(now, token).Return(false)

	allow := s.rateLimiter.Allow(now, req)
	s.False(allow)
}

func (s *priorityStageRateLimiterSuite) TestReserve_HighPriority_OK() {
	now := time.Now()
	token := 1
	req := quotas.Request{
		API:    s.highPriorityAPIName,
		Token:  token,
		Caller: "",
	}

	s.highPriorityReservation.EXPECT().OK().Return(true)
	s.highPriorityRateLimiter.EXPECT().ReserveN(now, token).Return(s.highPriorityReservation)
	s.lowPriorityRateLimiter.EXPECT().ReserveN(now, token).Return(s.lowPriorityReservation)

	reservation := s.rateLimiter.Reserve(now, req)
	s.Equal(quotas.NewPriorityReservation(
		s.highPriorityReservation,
		[]quotas.Reservation{s.lowPriorityReservation},
	), reservation)
}

func (s *priorityStageRateLimiterSuite) TestReserve_HighPriority_NotOK() {
	now := time.Now()
	token := 1
	req := quotas.Request{
		API:    s.highPriorityAPIName,
		Token:  token,
		Caller: "",
	}

	s.highPriorityReservation.EXPECT().OK().Return(false)
	s.highPriorityRateLimiter.EXPECT().ReserveN(now, token).Return(s.highPriorityReservation)

	reservation := s.rateLimiter.Reserve(now, req)
	s.Equal(s.highPriorityReservation, reservation)
}

func (s *priorityStageRateLimiterSuite) TestReserve_LowPriority_OK() {
	now := time.Now()
	token := 1
	req := quotas.Request{
		API:    s.lowPriorityAPIName,
		Token:  token,
		Caller: "",
	}

	s.lowPriorityReservation.EXPECT().OK().Return(true)
	s.lowPriorityRateLimiter.EXPECT().ReserveN(now, token).Return(s.lowPriorityReservation)

	reservation := s.rateLimiter.Reserve(now, req)
	s.Equal(quotas.NewPriorityReservation(
		s.lowPriorityReservation,
		[]quotas.Reservation{},
	), reservation)
}

func (s *priorityStageRateLimiterSuite) TestReserve_LowPriority_NotOK() {
	now := time.Now()
	token := 1
	req := quotas.Request{
		API:    s.lowPriorityAPIName,
		Token:  token,
		Caller: "",
	}

	s.lowPriorityReservation.EXPECT().OK().Return(false)
	s.lowPriorityRateLimiter.EXPECT().ReserveN(now, token).Return(s.lowPriorityReservation)

	reservation := s.rateLimiter.Reserve(now, req)
	s.Equal(s.lowPriorityReservation, reservation)
}

func (s *priorityStageRateLimiterSuite) TestWait_HighPriority_AlreadyExpired() {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	token := 1
	req := quotas.Request{
		API:    s.highPriorityAPIName,
		Token:  token,
		Caller: "",
	}

	err := s.rateLimiter.Wait(ctx, req)
	s.Error(err)
}

func (s *priorityStageRateLimiterSuite) TestWait_LowPriority_AlreadyExpired() {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	token := 1
	req := quotas.Request{
		API:    s.lowPriorityAPIName,
		Token:  token,
		Caller: "",
	}

	err := s.rateLimiter.Wait(ctx, req)
	s.Error(err)
}

func (s *priorityStageRateLimiterSuite) TestWait_HighPriority_NotExpired_WithExpiration_Error() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	token := 1
	req := quotas.Request{
		API:    s.highPriorityAPIName,
		Token:  token,
		Caller: "",
	}

	highPriorityReservationDelay := 2 * time.Second
	s.highPriorityReservation.EXPECT().DelayFrom(gomock.Any()).Return(highPriorityReservationDelay)
	s.highPriorityReservation.EXPECT().CancelAt(gomock.Any())
	s.lowPriorityReservation.EXPECT().CancelAt(gomock.Any())

	s.highPriorityReservation.EXPECT().OK().Return(true).AnyTimes()
	s.highPriorityRateLimiter.EXPECT().ReserveN(gomock.Any(), token).Return(s.highPriorityReservation)
	s.lowPriorityReservation.EXPECT().OK().Return(true).AnyTimes()
	s.lowPriorityRateLimiter.EXPECT().ReserveN(gomock.Any(), token).Return(s.lowPriorityReservation)

	err := s.rateLimiter.Wait(ctx, req)
	s.Error(err)
}

func (s *priorityStageRateLimiterSuite) TestWait_LowPriority_NotExpired_WithExpiration_Error() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	token := 1
	req := quotas.Request{
		API:    s.lowPriorityAPIName,
		Token:  token,
		Caller: "",
	}

	lowPriorityReservationDelay := 3 * time.Second
	s.lowPriorityReservation.EXPECT().DelayFrom(gomock.Any()).Return(lowPriorityReservationDelay)
	s.lowPriorityReservation.EXPECT().CancelAt(gomock.Any())

	s.lowPriorityReservation.EXPECT().OK().Return(true).AnyTimes()
	s.lowPriorityRateLimiter.EXPECT().ReserveN(gomock.Any(), token).Return(s.lowPriorityReservation)

	err := s.rateLimiter.Wait(ctx, req)
	s.Error(err)
}

func (s *priorityStageRateLimiterSuite) TestWait_HighPriority_NotExpired_WithExpiration_Cancelled() {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	token := 1
	req := quotas.Request{
		API:    s.highPriorityAPIName,
		Token:  token,
		Caller: "",
	}

	highPriorityReservationDelay := 20 * time.Second
	s.highPriorityReservation.EXPECT().DelayFrom(gomock.Any()).Return(highPriorityReservationDelay)
	s.highPriorityReservation.EXPECT().CancelAt(gomock.Any())
	s.lowPriorityReservation.EXPECT().CancelAt(gomock.Any())

	s.highPriorityReservation.EXPECT().OK().Return(true).AnyTimes()
	s.highPriorityRateLimiter.EXPECT().ReserveN(gomock.Any(), token).Return(s.highPriorityReservation)
	s.lowPriorityReservation.EXPECT().OK().Return(true).AnyTimes()
	s.lowPriorityRateLimiter.EXPECT().ReserveN(gomock.Any(), token).Return(s.lowPriorityReservation)

	go func() {
		s.clock.BlockUntil(1)
		s.clock.Advance(4 * time.Second)
		cancel()
	}()
	err := s.rateLimiter.Wait(ctx, req)
	s.Error(err)
}

func (s *priorityStageRateLimiterSuite) TestWait_LowPriority_NotExpired_WithExpiration_Cancelled() {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	token := 1
	req := quotas.Request{
		API:    s.lowPriorityAPIName,
		Token:  token,
		Caller: "",
	}

	lowPriorityReservationDelay := 30 * time.Second
	s.lowPriorityReservation.EXPECT().DelayFrom(gomock.Any()).Return(lowPriorityReservationDelay)
	s.lowPriorityReservation.EXPECT().CancelAt(gomock.Any())

	s.lowPriorityReservation.EXPECT().OK().Return(true).AnyTimes()
	s.lowPriorityRateLimiter.EXPECT().ReserveN(gomock.Any(), token).Return(s.lowPriorityReservation)

	go func() {
		s.clock.BlockUntil(1)
		s.clock.Advance(4 * time.Second)
		cancel()
	}()
	err := s.rateLimiter.Wait(ctx, req)
	s.Error(err)
}

func (s *priorityStageRateLimiterSuite) TestWait_HighPriority_NotExpired_WithExpiration_NoError() {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	token := 1
	req := quotas.Request{
		API:    s.highPriorityAPIName,
		Token:  token,
		Caller: "",
	}

	highPriorityReservationDelay := 2 * time.Second
	s.highPriorityReservation.EXPECT().DelayFrom(gomock.Any()).Return(highPriorityReservationDelay)

	s.highPriorityReservation.EXPECT().OK().Return(true).AnyTimes()
	s.highPriorityRateLimiter.EXPECT().ReserveN(gomock.Any(), token).Return(s.highPriorityReservation)
	s.lowPriorityReservation.EXPECT().OK().Return(true).AnyTimes()
	s.lowPriorityRateLimiter.EXPECT().ReserveN(gomock.Any(), token).Return(s.lowPriorityReservation)

	go func() {
		s.clock.BlockUntil(1)
		s.clock.Advance(highPriorityReservationDelay)
	}()
	err := s.rateLimiter.Wait(ctx, req)
	s.NoError(err)
}

func (s *priorityStageRateLimiterSuite) TestWait_LowPriority_NotExpired_WithExpiration_NoError() {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	token := 1
	req := quotas.Request{
		API:    s.lowPriorityAPIName,
		Token:  token,
		Caller: "",
	}

	lowPriorityReservationDelay := 2 * time.Second
	s.lowPriorityReservation.EXPECT().DelayFrom(gomock.Any()).Return(lowPriorityReservationDelay)

	s.lowPriorityReservation.EXPECT().OK().Return(true).AnyTimes()
	s.lowPriorityRateLimiter.EXPECT().ReserveN(gomock.Any(), token).Return(s.lowPriorityReservation)

	go func() {
		s.clock.BlockUntil(1)
		s.clock.Advance(lowPriorityReservationDelay)
	}()
	err := s.rateLimiter.Wait(ctx, req)
	s.NoError(err)
}

func (s *priorityStageRateLimiterSuite) TestWait_HighPriority_NotExpired_WithoutExpiration() {
	ctx := context.Background()
	token := 1
	req := quotas.Request{
		API:    s.highPriorityAPIName,
		Token:  token,
		Caller: "",
	}

	highPriorityReservationDelay := 2 * time.Second
	s.highPriorityReservation.EXPECT().DelayFrom(gomock.Any()).Return(highPriorityReservationDelay)

	s.highPriorityReservation.EXPECT().OK().Return(true).AnyTimes()
	s.highPriorityRateLimiter.EXPECT().ReserveN(gomock.Any(), token).Return(s.highPriorityReservation)
	s.lowPriorityReservation.EXPECT().OK().Return(true).AnyTimes()
	s.lowPriorityRateLimiter.EXPECT().ReserveN(gomock.Any(), token).Return(s.lowPriorityReservation)

	go func() {
		s.clock.BlockUntil(1)
		s.clock.Advance(highPriorityReservationDelay)
	}()
	err := s.rateLimiter.Wait(ctx, req)
	s.NoError(err)
}

func (s *priorityStageRateLimiterSuite) TestWait_LowPriority_NotExpired_WithoutExpiration() {
	ctx := context.Background()
	token := 1
	req := quotas.Request{
		API:    s.lowPriorityAPIName,
		Token:  token,
		Caller: "",
	}

	lowPriorityReservationDelay := 3 * time.Second
	s.lowPriorityReservation.EXPECT().DelayFrom(gomock.Any()).Return(lowPriorityReservationDelay)

	s.lowPriorityReservation.EXPECT().OK().Return(true).AnyTimes()
	s.lowPriorityRateLimiter.EXPECT().ReserveN(gomock.Any(), token).Return(s.lowPriorityReservation)

	go func() {
		s.clock.BlockUntil(1)
		s.clock.Advance(lowPriorityReservationDelay)
	}()
	err := s.rateLimiter.Wait(ctx, req)
	s.NoError(err)
}
