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

func (s *callerInfoSuite) TestSetCallerInfo_PreserveOtherValues() {
	existingKey := "key"
	existingValue := "value"
	callerName := "callerName"
	callerType := CallerTypeAPI

	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs(existingKey, existingValue),
	)

	ctx = SetCallerInfo(ctx, callerName, callerType)

	md, ok := metadata.FromIncomingContext(ctx)
	s.True(ok)
	s.Equal(existingValue, md.Get(existingKey)[0])
	s.Equal(callerName, md.Get(CallerNameHeaderName)[0])
	s.Equal(callerType, md.Get(CallerTypeHeaderName)[0])
	s.Len(md, 3)
}

func (s *callerInfoSuite) TestSetCallerInfo_NoExistingCallerInfo() {
	callerName := "callerName"
	callerType := CallerTypeAPI

	ctx := SetCallerInfo(context.Background(), callerName, callerType)

	md, ok := metadata.FromIncomingContext(ctx)
	s.True(ok)
	s.Equal(callerName, md.Get(CallerNameHeaderName)[0])
	s.Equal(callerType, md.Get(CallerTypeHeaderName)[0])
	s.Len(md, 2)
}

func (s *callerInfoSuite) TestSetCallerInfo_WithExistingCallerInfo() {
	callerName := "callerName"
	callerType := CallerTypeAPI

	ctx := SetCallerInfo(context.Background(), callerName, callerType)

	ctx = SetCallerInfo(ctx, "another caller", CallerTypeBackground)

	md, ok := metadata.FromIncomingContext(ctx)
	s.True(ok)
	s.Equal(callerName, md.Get(CallerNameHeaderName)[0])
	s.Equal(callerType, md.Get(CallerTypeHeaderName)[0])
	s.Len(md, 2)
}

func (s *callerInfoSuite) TestSetCallerInfo_WithPartialCallerInfo() {
	callerName := "callerName"
	callerType := CallerTypeBackground

	ctx := SetCallerInfo(context.Background(), callerName, "")
	ctx = SetCallerInfo(ctx, "another caller", callerType)

	md, ok := metadata.FromIncomingContext(ctx)
	s.True(ok)
	s.Equal(callerName, md.Get(CallerNameHeaderName)[0])
	s.Equal(callerType, md.Get(CallerTypeHeaderName)[0])
	s.Len(md, 2)

	ctx = SetCallerInfo(context.Background(), "", callerType)
	ctx = SetCallerInfo(ctx, callerName, "")

	md, ok = metadata.FromIncomingContext(ctx)
	s.True(ok)
	s.Equal(callerName, md.Get(CallerNameHeaderName)[0])
	s.Equal(callerType, md.Get(CallerTypeHeaderName)[0])
	s.Len(md, 2)
}
