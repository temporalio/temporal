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
	t := time.Now()
	ts := TimestampFromTime(&t)
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
	a := time.Now().Add(time.Nanosecond)

	before := TimestampFromProto(t)
	after := TimestampFromTime(&a)
	s.beforeAfterValidation(before, after)
}

func (s *TimestampSuite) TestTimestampBeforeAfterGoProto() {
	t := time.Now()
	a := types.TimestampNow()
	a.Nanos += 1

	before := TimestampFromTime(&t)
	after := TimestampFromProto(a)
	s.beforeAfterValidation(before, after)
}

func (s *TimestampSuite) TestTimestampBeforeAfterGoGo() {
	t := time.Now()
	a := t.Add(time.Nanosecond)
	s.NotEqual(t.UnixNano(), a.UnixNano())

	before := TimestampFromTime(&t)
	after := TimestampFromTime(&a)
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
