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

package timestamp

import (
	"testing"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/suite"
)

// Timestamp suite tests time
type TimestampSuite struct {
	suite.Suite
}

func TestTimestampSuite(t *testing.T) {
	suite.Run(t, new(TimestampSuite))
}

func (s *TimestampSuite) TestCreateTimestampFromGoToProto() {
	t := time.Now().UTC()
	ts := TimestampFromTimePtr(&t)
	tp := ts.ToProto()
	t2, err := types.TimestampFromProto(tp)
	s.NoError(err)
	s.EqualValues(t.UTC(), t2)
}

func (s *TimestampSuite) TestCreateTimestampFromProtoToGo() {
	t := types.TimestampNow()
	ts := TimestampFromProto(t)
	tt := ts.ToTime()
	t2, err := types.TimestampProto(*tt)
	s.NoError(err)
	s.EqualValues(t, t2)
}

func (s *TimestampSuite) TestTimestampBeforeAfterProtoProto() {
	t := types.TimestampNow()
	a := *t
	a.Nanos += 1
	s.NotEqual(*t, a)

	before := TimestampFromProto(t)
	after := TimestampFromProto(&a)
	s.beforeAfterValidation(before, after)
}

func (s *TimestampSuite) TestTimestampBeforeAfterProtoGo() {
	t := types.TimestampNow()
	a := time.Now().UTC().Add(time.Nanosecond)

	before := TimestampFromProto(t)
	after := TimestampFromTimePtr(&a)
	s.beforeAfterValidation(before, after)
}

func (s *TimestampSuite) TestTimestampBeforeAfterGoProto() {
	t := time.Now().UTC()
	a := types.TimestampNow()
	a.Nanos += 1

	before := TimestampFromTimePtr(&t)
	after := TimestampFromProto(a)
	s.beforeAfterValidation(before, after)
}

func (s *TimestampSuite) TestTimestampBeforeAfterGoGo() {
	t := time.Now().UTC()
	a := t.Add(time.Nanosecond)
	s.NotEqual(t.UnixNano(), a.UnixNano())

	before := TimestampFromTimePtr(&t)
	after := TimestampFromTimePtr(&a)
	s.beforeAfterValidation(before, after)
}

func (s *TimestampSuite) beforeAfterValidation(before *Timestamp, after *Timestamp) {
	s.True(before.Before(after))
	s.True(after.After(before))

	s.False(after.Before(before))
	s.False(before.After(after))

	s.False(after.SameAs(before))
	s.False(before.SameAs(after))
}
