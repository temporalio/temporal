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

func (s *failpointSuite) TestOn() {
	fp := failpoint.New[int](intKey)
	s.Equal(failpoint.State{Key: intKey, Action: failpoint.ActionNone}, fp.State())

	fp.On()
	s.Equal(failpoint.State{Key: intKey, Action: failpoint.ActionOn}, fp.State())
}

func (s *failpointSuite) TestDisable() {
	fp := failpoint.New[int](intKey)
	fp.On()
	fp.Disable()
	s.Equal(failpoint.State{Key: intKey, Action: failpoint.ActionNone}, fp.State())
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
	s.Equal("On", failpoint.ActionOn.String())
	s.Equal("Action(99)", failpoint.Action(99).String())
}
