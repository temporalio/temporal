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
	fp := failpoint.New[int](intKey)
	fp.Return(43)
	s.Equal(failpoint.State{Key: intKey, Action: failpoint.ActionReturn}, fp.State())
	for range 5 {
		got, set := fp.Execute()
		s.True(set)
		s.Equal(43, got)
	}
}

func (s *failpointSuite) TestOn() {
	fp := failpoint.New[int](intKey)
	fp.Return(42)
	fp.On()
	s.Equal(failpoint.State{Key: intKey, Action: failpoint.ActionOn}, fp.State())

	got, set := fp.Execute()
	s.True(set)
	s.Zero(got, "On does not return the value of an earlier Return")
}

func (s *failpointSuite) TestDisable() {
	fp := failpoint.New[int](intKey)
	fp.Return(42)
	fp.Disable()
	s.Equal(failpoint.State{Key: intKey, Action: failpoint.ActionNone}, fp.State())

	got, set := fp.Execute()
	s.False(set)
	s.Zero(got)
}

func (s *failpointSuite) TestTotalFires() {
	registry := failpoint.NewRegistry()
	fp := failpoint.New[int](intKey)
	registry.Set(fp)

	s.False(intKey.Trip(registry))
	s.Zero(fp.TotalFires(), "a failpoint that is off doesn't fire")

	fp.On()
	s.True(intKey.Trip(registry))
	s.True(intKey.Trip(registry))
	s.EqualValues(2, fp.TotalFires())

	fp.Disable()
	s.False(intKey.Trip(registry))
	s.EqualValues(2, fp.TotalFires(), "disabling keeps the count")
}

func (s *failpointSuite) TestActionString() {
	s.Equal("None", failpoint.ActionNone.String())
	s.Equal("Return", failpoint.ActionReturn.String())
	s.Equal("On", failpoint.ActionOn.String())
	s.Equal("Action(99)", failpoint.Action(99).String())
}
