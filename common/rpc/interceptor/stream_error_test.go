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

package interceptor

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type (
	streamErrorSuite struct {
		*require.Assertions
		suite.Suite
	}
)

func TestStreamErrorSuite(t *testing.T) {
	s := new(streamErrorSuite)
	suite.Run(t, s)
}

func (s *streamErrorSuite) SetupSuite() {
	s.Assertions = require.New(s.T())
}

func (s *streamErrorSuite) TearDownSuite() {
}

func (s *streamErrorSuite) SetupTest() {
}

func (s *streamErrorSuite) TearDownTest() {
}

func (s *streamErrorSuite) TestErrorConversion() {
	s.Equal(nil, errorConvert(nil))
	s.Equal(io.EOF, errorConvert(io.EOF))

	s.IsType(nil, errorConvert(status.Error(codes.OK, "")))
	s.IsType(&serviceerror.DeadlineExceeded{}, errorConvert(status.Error(codes.DeadlineExceeded, "")))
	s.IsType(&serviceerror.Canceled{}, errorConvert(status.Error(codes.Canceled, "")))
	s.IsType(&serviceerror.InvalidArgument{}, errorConvert(status.Error(codes.InvalidArgument, "")))
	s.IsType(&serviceerror.FailedPrecondition{}, errorConvert(status.Error(codes.FailedPrecondition, "")))
	s.IsType(&serviceerror.Unavailable{}, errorConvert(status.Error(codes.Unavailable, "")))
	s.IsType(&serviceerror.Internal{}, errorConvert(status.Error(codes.Internal, "")))
	s.IsType(&serviceerror.Internal{}, errorConvert(status.Error(codes.Unknown, "")))
}
