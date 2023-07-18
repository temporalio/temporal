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

package headers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/metadata"
)

type (
	callerInfoSuite struct {
		*require.Assertions
		suite.Suite
	}
)

func TestCallerInfoSuite(t *testing.T) {
	suite.Run(t, &callerInfoSuite{})
}

func (s *callerInfoSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *callerInfoSuite) TestSetCallerName() {
	ctx := context.Background()
	info := GetCallerInfo(ctx)
	s.Empty(info.CallerName)

	ctx = SetCallerName(ctx, CallerNameSystem)
	info = GetCallerInfo(ctx)
	s.Equal(CallerNameSystem, info.CallerName)

	ctx = SetCallerName(ctx, "")
	info = GetCallerInfo(ctx)
	s.Equal(CallerNameSystem, info.CallerName)

	newCallerName := "new caller name"
	ctx = SetCallerName(ctx, newCallerName)
	info = GetCallerInfo(ctx)
	s.Equal(newCallerName, info.CallerName)
}

func (s *callerInfoSuite) TestSetCallerType() {
	ctx := context.Background()
	info := GetCallerInfo(ctx)
	s.Empty(info.CallerType)

	ctx = SetCallerType(ctx, CallerTypeBackground)
	info = GetCallerInfo(ctx)
	s.Equal(CallerTypeBackground, info.CallerType)

	ctx = SetCallerName(ctx, "")
	info = GetCallerInfo(ctx)
	s.Equal(CallerTypeBackground, info.CallerType)

	ctx = SetCallerType(ctx, CallerTypeAPI)
	info = GetCallerInfo(ctx)
	s.Equal(CallerTypeAPI, info.CallerType)

	ctx = SetCallerType(ctx, CallerTypePreemptable)
	info = GetCallerInfo(ctx)
	s.Equal(CallerTypePreemptable, info.CallerType)
}

func (s *callerInfoSuite) TestSetCallOrigin() {
	ctx := context.Background()
	info := GetCallerInfo(ctx)
	s.Empty(info.CallOrigin)

	initiation := "method name"
	ctx = SetOrigin(ctx, initiation)
	info = GetCallerInfo(ctx)
	s.Equal(initiation, info.CallOrigin)

	ctx = SetOrigin(ctx, "")
	info = GetCallerInfo(ctx)
	s.Equal(initiation, info.CallOrigin)

	newCallOrigin := "another method name"
	ctx = SetOrigin(ctx, newCallOrigin)
	info = GetCallerInfo(ctx)
	s.Equal(newCallOrigin, info.CallOrigin)
}

func (s *callerInfoSuite) TestSetCallerInfo_PreserveOtherValues() {
	existingKey := "key"
	existingValue := "value"
	callerName := "callerName"
	callerType := CallerTypeAPI
	callOrigin := "methodName"

	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs(existingKey, existingValue),
	)

	ctx = SetCallerInfo(ctx, NewCallerInfo(callerName, callerType, callOrigin))

	md, ok := metadata.FromIncomingContext(ctx)
	s.True(ok)
	s.Equal(existingValue, md.Get(existingKey)[0])
	s.Equal(callerName, md.Get(callerNameHeaderName)[0])
	s.Equal(callerType, md.Get(CallerTypeHeaderName)[0])
	s.Equal(callOrigin, md.Get(callOriginHeaderName)[0])
	s.Len(md, 4)
}

func (s *callerInfoSuite) TestSetCallerInfo_NoExistingCallerInfo() {
	callerName := CallerNameSystem
	callerType := CallerTypeAPI
	callOrigin := "methodName"

	ctx := SetCallerInfo(context.Background(), CallerInfo{
		CallerName: callerName,
		CallerType: callerType,
		CallOrigin: callOrigin,
	})

	md, ok := metadata.FromIncomingContext(ctx)
	s.True(ok)
	s.Equal(callerName, md.Get(callerNameHeaderName)[0])
	s.Equal(callerType, md.Get(CallerTypeHeaderName)[0])
	s.Equal(callOrigin, md.Get(callOriginHeaderName)[0])
	s.Len(md, 3)
}

func (s *callerInfoSuite) TestSetCallerInfo_WithExistingCallerInfo() {
	callerName := CallerNameSystem
	callerType := CallerTypeBackground
	callOrigin := "methodName"

	ctx := SetCallerName(context.Background(), callerName)
	ctx = SetCallerType(ctx, CallerTypeAPI)
	ctx = SetOrigin(ctx, callOrigin)

	ctx = SetCallerInfo(ctx, CallerInfo{
		CallerName: "",
		CallerType: callerType,
		CallOrigin: "",
	})

	md, ok := metadata.FromIncomingContext(ctx)
	s.True(ok)
	s.Equal(callerName, md.Get(callerNameHeaderName)[0])
	s.Equal(callerType, md.Get(CallerTypeHeaderName)[0])
	s.Equal(callOrigin, md.Get(callOriginHeaderName)[0])
	s.Len(md, 3)
}

func (s *callerInfoSuite) TestSetCallerInfo_WithPartialCallerInfo() {
	callerName := CallerNameSystem
	callerType := CallerTypeAPI

	ctx := SetCallerType(context.Background(), callerType)

	ctx = SetCallerInfo(ctx, CallerInfo{
		CallerName: callerName,
	})

	md, ok := metadata.FromIncomingContext(ctx)
	s.True(ok)
	s.Equal(callerName, md.Get(callerNameHeaderName)[0])
	s.Equal(callerType, md.Get(CallerTypeHeaderName)[0])
	s.Empty(md.Get(callOriginHeaderName))
	s.Len(md, 2)
}
