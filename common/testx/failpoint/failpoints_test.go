package failpoint_test

import (
	"errors"
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

// do returns the value that the failpoint sets, or 66 if it doesn't set one.
func do(f *failpoint.Failpoint[int]) int {
	if v, set := f.Execute(); set {
		return v
	}
	return 66
}

func fires(fp *failpoint.Failpoint[error], executions int) []bool {
	out := make([]bool, executions)
	for i := range out {
		_, out[i] = fp.Execute()
	}
	return out
}

func (s *failpointSuite) TestReturn() {
	s.Run("once", func(s *failpointSuite) {
		f := new(failpoint.Failpoint[int])
		f.Return(42, failpoint.Trigger{MaxFires: 1})
		s.Equal(42, do(f))
		s.Equal(66, do(f))
	})

	s.Run("always", func(s *failpointSuite) {
		f := new(failpoint.Failpoint[int])
		f.Return(43)
		for range 10 {
			s.Equal(43, do(f))
		}
	})
}

func (s *failpointSuite) TestOn() {
	fp := failpoint.NewFailpoint[int](intKey)
	fp.Return(42)
	fp.On(failpoint.Trigger{MaxFires: 1})
	s.Equal(failpoint.State{Key: intKey, Action: failpoint.ActionOn, Trigger: failpoint.Trigger{MaxFires: 1}}, fp.State())

	got, fired := fp.Execute()
	s.True(fired)
	s.Zero(got, "On does not return the value of an earlier Return")
	_, fired = fp.Execute()
	s.False(fired)
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
	s.Equal("Off", failpoint.ActionOff.String())
	s.Equal("Return", failpoint.ActionReturn.String())
	s.Equal("On", failpoint.ActionOn.String())
	s.Equal("Action(99)", failpoint.Action(99).String())
}

func (s *failpointSuite) TestTrigger() {
	injected := errors.New("injected")
	basicTests := []struct {
		name    string
		trigger failpoint.Trigger
		want    []bool
	}{
		{name: "zero value fires always", trigger: failpoint.Trigger{}, want: []bool{true, true, true, true}},
		{name: "skip first skips executions", trigger: failpoint.Trigger{SkipFirst: 2}, want: []bool{false, false, true, true}},
		{name: "max fires limits fires", trigger: failpoint.Trigger{MaxFires: 2}, want: []bool{true, true, false, false}},
		{name: "skip first and max fires", trigger: failpoint.Trigger{SkipFirst: 1, MaxFires: 1}, want: []bool{false, true, false, false}},
	}
	for _, tc := range basicTests {
		s.Run(tc.name, func(s *failpointSuite) {
			fp := failpoint.NewFailpoint[error](key1)
			fp.Return(injected)
			s.NoError(fp.SetTrigger(tc.trigger))
			s.Equal(tc.want, fires(fp, len(tc.want)))
		})
	}

	invalidTests := []struct {
		name    string
		trigger failpoint.Trigger
	}{
		{name: "rejects negative skip first", trigger: failpoint.Trigger{SkipFirst: -1}},
		{name: "rejects negative max fires", trigger: failpoint.Trigger{MaxFires: -1}},
	}
	for _, tc := range invalidTests {
		s.Run(tc.name, func(s *failpointSuite) {
			fp := failpoint.NewFailpoint[error](key1)
			fp.Return(errors.New("injected"))
			s.NoError(fp.SetTrigger(failpoint.Trigger{MaxFires: 1}))
			s.Equal([]bool{true}, fires(fp, 1))

			s.Error(fp.SetTrigger(tc.trigger))
			// The previous trigger and its counts are unchanged.
			executions, fired := fp.Stats()
			s.Equal([2]int64{1, 1}, [2]int64{executions, fired})
			s.Equal([]bool{false}, fires(fp, 1))
		})
	}

	s.Run("skipped execution does not consume max fires", func(s *failpointSuite) {
		fp := failpoint.NewFailpoint[error](key1)
		fp.Return(errors.New("injected"), failpoint.Trigger{MaxFires: 1, SkipFirst: 1})
		s.Equal([]bool{false, true, false}, fires(fp, 3))

		executions, fired := fp.Stats()
		s.Equal(int64(3), executions, "executions after the last fire still count")
		s.Equal(int64(1), fired)
	})

	s.Run("total fires survives set trigger", func(s *failpointSuite) {
		fp := failpoint.NewFailpoint[error](key1)
		fp.Return(errors.New("injected"))
		s.Zero(fp.TotalFires())
		s.Equal([]bool{true, true}, fires(fp, 2))

		s.NoError(fp.SetTrigger(failpoint.Trigger{MaxFires: 1}))
		_, fired := fp.Stats()
		s.Zero(fired, "Stats is relative to the trigger")
		s.Equal(int64(2), fp.TotalFires())

		s.Equal([]bool{true, false}, fires(fp, 2))
		s.Equal(int64(3), fp.TotalFires())
	})

	s.Run("action setter replaces trigger", func(s *failpointSuite) {
		fp := failpoint.NewFailpoint[error](key1)
		fp.Return(errors.New("once"), failpoint.Trigger{MaxFires: 1})
		s.Equal([]bool{true, false}, fires(fp, 2))

		// Configuring the action again starts over: without a trigger it fires on every execution.
		fp.Return(errors.New("always"))
		s.Equal(failpoint.Trigger{}, fp.State().Trigger)
		s.Equal([]bool{true, true, true}, fires(fp, 3))
		executions, fired := fp.Stats()
		s.Equal([2]int64{3, 3}, [2]int64{executions, fired})
		s.Equal(int64(4), fp.TotalFires())
	})

	s.Run("action setter rejects invalid trigger", func(s *failpointSuite) {
		fp := failpoint.NewFailpoint[error](key1)
		s.Panics(func() { fp.Return(errors.New("x"), failpoint.Trigger{SkipFirst: -1}) })
		s.Panics(func() { fp.On(failpoint.Trigger{}, failpoint.Trigger{}) })
		s.Equal(failpoint.ActionOff, fp.State().Action, "a rejected setter changes nothing")
	})
}
