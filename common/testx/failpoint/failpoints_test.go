package failpoint_test

import (
	"testing"

	"go.temporal.io/testx/failpoint"
	"go.temporal.io/testx/parallelsuite"
)

type failpointSuite struct {
	parallelsuite.Suite[*failpointSuite]
}

func TestFailpointSuite(t *testing.T) {
	parallelsuite.Run(t, &failpointSuite{})
}

func (s *failpointSuite) TestReturn() {
	fp := failpoint.NewFailpoint[int](intKey)
	fp.Return(43)
	s.Equal(failpoint.State{Key: intKey, Action: failpoint.ActionReturn}, fp.State())
	for range 5 {
		got, fired := fp.Execute()
		s.True(fired)
		s.Equal(43, got)
	}
}

func (s *failpointSuite) TestOn() {
	fp := failpoint.NewFailpoint[int](intKey)
	fp.Return(42)
	fp.On()
	s.Equal(failpoint.State{Key: intKey, Action: failpoint.ActionOn}, fp.State())

	got, fired := fp.Execute()
	s.True(fired)
	s.Zero(got, "On does not return the value of an earlier Return")
}

func (s *failpointSuite) TestDisable() {
	fp := failpoint.NewFailpoint[int](intKey)
	fp.Return(42)
	fp.Disable()
	s.Equal(failpoint.State{Key: intKey, Action: failpoint.ActionOff}, fp.State())

	got, fired := fp.Execute()
	s.False(fired)
	s.Zero(got)
}

func (s *failpointSuite) TestActionString() {
	s.Equal("Unspecified", failpoint.ActionUnspecified.String())
	s.Equal("Off", failpoint.ActionOff.String())
	s.Equal("Return", failpoint.ActionReturn.String())
	s.Equal("On", failpoint.ActionOn.String())
	s.Equal("Action(99)", failpoint.Action(99).String())
}
