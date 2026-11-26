package failpoint_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

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

func (s *failpointSuite) TestPanic() {
	s.Run("once", func(s *failpointSuite) {
		f := new(failpoint.Failpoint[int])
		f.Panic(42, failpoint.Trigger{MaxFires: 1})
		s.PanicsWithValue(42, func() { do(f) })
		s.Equal(66, do(f))
	})

	s.Run("always", func(s *failpointSuite) {
		f := new(failpoint.Failpoint[int])
		f.Panic(43)
		for range 10 {
			s.PanicsWithValue(43, func() { do(f) })
		}
	})
}

func (s *failpointSuite) TestBlock() {
	s.Run("wait returns error when not blocking", func(s *failpointSuite) {
		f := new(failpoint.Failpoint[struct{}])
		s.ErrorIs(f.Wait(s.Context()), failpoint.ErrNotBlocking)
		f.On()
		s.ErrorIs(f.Wait(s.Context()), failpoint.ErrNotBlocking)
	})

	s.Run("wait before arrival", func(s *failpointSuite) {
		f := new(failpoint.Failpoint[struct{}])
		f.Block()

		done := make(chan struct{})
		go func() {
			defer close(done)
			f.Execute()
		}()

		s.NoError(f.Wait(s.Context()))
		f.Disable()
		s.Rcv(done)
	})

	s.Run("wait after arrival", func(s *failpointSuite) {
		f := new(failpoint.Failpoint[struct{}])
		f.Block()

		arrived := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			close(arrived)
			f.Execute()
		}()

		s.Rcv(arrived)
		s.NoError(f.Wait(s.Context()))
		f.Disable()
		s.Rcv(done)
	})

	s.Run("wait ctx cancelled", func(s *failpointSuite) {
		f := new(failpoint.Failpoint[struct{}])
		f.Block()

		ctx, cancel := context.WithCancel(s.Context())
		cancel()

		s.ErrorIs(f.Wait(ctx), context.Canceled)
		f.Disable()
	})

	s.Run("disable before arrival returns error", func(s *failpointSuite) {
		s.Synctest(func(s *failpointSuite) {
			f := new(failpoint.Failpoint[struct{}])
			f.Block()

			waited := make(chan error, 1)
			go func() { waited <- f.Wait(s.Context()) }()
			synctest.Wait()

			f.Disable()
			s.ErrorIs(s.Rcv(waited), failpoint.ErrNotBlocking)
		})
	})

	s.Run("rearm before arrival returns error", func(s *failpointSuite) {
		s.Synctest(func(s *failpointSuite) {
			f := new(failpoint.Failpoint[struct{}])
			f.Block()

			waited := make(chan error, 1)
			go func() { waited <- f.Wait(s.Context()) }()
			synctest.Wait()

			f.Block()
			s.ErrorIs(s.Rcv(waited), failpoint.ErrNotBlocking)
			f.Disable()
		})
	})

	// Calling Block while a goroutine is blocked wakes it via disableLocked closing the old blockCh.
	s.Run("block called twice wakes blocked goroutine", func(s *failpointSuite) {
		f := new(failpoint.Failpoint[struct{}])
		f.Block()

		done := make(chan struct{})
		go func() {
			defer close(done)
			f.Execute()
		}()

		s.NoError(f.Wait(s.Context()))

		// Second Block() must wake the goroutine blocked on the old blockCh.
		f.Block()
		s.Rcv(done)
		f.Disable()
	})

	s.Run("block called twice still functional", func(s *failpointSuite) {
		f := new(failpoint.Failpoint[struct{}])
		f.Block()
		f.Block()

		done := make(chan struct{})
		go func() {
			defer close(done)
			f.Execute()
		}()

		s.NoError(f.Wait(s.Context()))
		f.Disable()
		s.Rcv(done)
	})

	// Guards against a prior race where the cleanup of a block that fires once would clobber state
	// belonging to a fresh Block() that landed during the wait.
	s.Run("once block concurrent reinject", func(s *failpointSuite) {
		fp := failpoint.NewFailpoint[error](key1)
		fp.Block(failpoint.Trigger{MaxFires: 1})

		executed := make(chan struct{})
		go func() {
			defer close(executed)
			_, _ = fp.Execute()
		}()

		s.NoError(fp.Wait(s.Context()))

		// Re-inject an unlimited Block while the original goroutine is parked.
		fp.Block()
		s.Rcv(executed)

		// The new failpoint must still be usable: a fresh goroutine should park and Disable releases it.
		parked := make(chan struct{})
		go func() {
			defer close(parked)
			_, _ = fp.Execute()
		}()
		s.NoError(fp.Wait(s.Context()))
		fp.Disable()
		s.Rcv(parked)
	})

	s.Run("max fires one holds one execution", func(s *failpointSuite) {
		fp := failpoint.NewFailpoint[error](key1)
		fp.Block(failpoint.Trigger{MaxFires: 1})
		s.T().Cleanup(fp.Disable)

		released := make(chan struct{}, 1)
		go func() {
			_, _ = fp.Execute()
			released <- struct{}{}
		}()
		s.NoError(fp.Wait(s.Context()))

		// The next executions pass, and Wait returns nil while the one execution is held.
		s.Equal([]bool{false, false}, fires(fp, 2))
		s.NoError(fp.Wait(s.Context()))
		s.Equal(int64(1), fp.TotalFires())

		fp.Disable()
		s.Rcv(released)
	})

	s.Run("set trigger releases held execution", func(s *failpointSuite) {
		fp := failpoint.NewFailpoint[error](key1)
		fp.Block(failpoint.Trigger{MaxFires: 1})
		s.T().Cleanup(fp.Disable)

		first := make(chan struct{}, 1)
		go func() {
			_, _ = fp.Execute()
			first <- struct{}{}
		}()
		s.NoError(fp.Wait(s.Context()))

		// Replacing the trigger starts over: it releases the held execution, so the next one is the
		// only one that the new block holds.
		s.NoError(fp.SetTrigger(failpoint.Trigger{MaxFires: 1}))
		s.Rcv(first)

		second := make(chan struct{}, 1)
		go func() {
			_, _ = fp.Execute()
			second <- struct{}{}
		}()
		s.NoError(fp.Wait(s.Context()))

		// Executions after the second one pass, so exactly one execution is held.
		s.Equal([]bool{false, false}, fires(fp, 2))
		select {
		case <-second:
			s.Fail("the second execution was released")
		default:
		}

		fp.Disable()
		s.Rcv(second)
	})
}

func (s *failpointSuite) TestCall() {
	s.Run("once", func(s *failpointSuite) {
		f := new(failpoint.Failpoint[int])
		f.Call(func() int {
			return 42
		}, failpoint.Trigger{MaxFires: 1})
		s.Equal(42, do(f))
		s.Equal(66, do(f))
	})

	s.Run("always", func(s *failpointSuite) {
		f := new(failpoint.Failpoint[int])
		f.Call(func() int {
			return 44
		})
		for range 10 {
			s.Equal(44, do(f))
		}
	})

	s.Run("call with arg", func(s *failpointSuite) {
		fp := failpoint.NewFailpoint[error](key1)
		var args []any
		fp.CallWithArg(func(arg any) error {
			args = append(args, arg)
			return nil
		})

		_, fired := fp.Execute()
		s.True(fired)

		registry := failpoint.NewRegistry()
		registry.Set(fp)
		res := failpoint.ExecuteInOrder(registry, failpoint.ExecuteOptions{Arg: "request"}, key1)
		s.True(res.Fired)
		s.True(res.Set)
		s.NoError(res.Value)
		s.Equal([]any{nil, "request"}, args)
	})

	s.Run("concurrent executions overlap", func(s *failpointSuite) {
		f := failpoint.NewFailpoint[int](failpoint.UndeclaredKey[int]("overlap"))
		arrived := make(chan struct{})
		release := make(chan struct{})
		f.Call(func() int {
			arrived <- struct{}{}
			<-release
			return 42
		})

		results := make(chan int, 2)
		for range 2 {
			go func() {
				v, _ := f.Execute()
				results <- v
			}()
		}
		for range 2 {
			s.Rcv(arrived)
		}
		close(release)
		s.Equal(42, s.Rcv(results))
		s.Equal(42, s.Rcv(results))
	})

	s.Run("callback calls into failpoint", func(s *failpointSuite) {
		f := failpoint.NewFailpoint[int](failpoint.UndeclaredKey[int]("reentrant"))
		f.Call(func() int {
			f.Disable()
			return 42
		})

		done := make(chan int, 1)
		go func() {
			v, _ := f.Execute()
			done <- v
		}()
		s.Equal(42, s.Rcv(done))
		_, fired := f.Execute()
		s.False(fired)
	})

	s.Run("once reconfigured during callback", func(s *failpointSuite) {
		f := failpoint.NewFailpoint[int](failpoint.UndeclaredKey[int]("once-rearm"))
		entered := make(chan struct{})
		release := make(chan struct{})
		f.Call(func() int {
			close(entered)
			<-release
			return 1
		}, failpoint.Trigger{MaxFires: 1})

		first := make(chan int, 1)
		go func() {
			v, _ := f.Execute()
			first <- v
		}()
		s.Rcv(entered)

		// The running execution consumed the MaxFires: 1, so a concurrent execution doesn't fire.
		_, fired := f.Execute()
		s.False(fired)

		// Re-arm while the first callback still runs.
		f.Call(func() int { return 2 }, failpoint.Trigger{MaxFires: 1})
		close(release)
		s.Equal(1, s.Rcv(first))

		// The first callback's completion did not disable the replacement.
		v, fired := f.Execute()
		s.True(fired)
		s.Equal(2, v)
		_, fired = f.Execute()
		s.False(fired)
	})
}

func (s *failpointSuite) TestPause() {
	tests := []struct {
		name    string
		trigger failpoint.Trigger
		release func(*failpoint.Failpoint[error])
	}{
		{name: "ended by disable", release: (*failpoint.Failpoint[error]).Disable},
		{name: "ended by disable once", trigger: failpoint.Trigger{MaxFires: 1}, release: (*failpoint.Failpoint[error]).Disable},
		{name: "ended by reconfigure", release: func(fp *failpoint.Failpoint[error]) {
			fp.Return(errors.New("x"))
		}},
	}
	for _, tc := range tests {
		s.Run(tc.name, func(s *failpointSuite) {
			s.Synctest(func(s *failpointSuite) {
				fp := failpoint.NewFailpoint[error](key1)
				fp.Pause(time.Hour, tc.trigger)
				done := make(chan struct{})
				go func() {
					defer close(done)
					_, _ = fp.Execute()
				}()

				// Verify the execution is genuinely parked in the pause before release.
				synctest.Wait()
				select {
				case <-done:
					s.Fail("pause returned before release")
				default:
				}

				tc.release(fp)
				synctest.Wait()
				s.Rcv(done)
			})
		})
	}

	s.Run("expires after duration", func(s *failpointSuite) {
		s.Synctest(func(s *failpointSuite) {
			fp := failpoint.NewFailpoint[error](key1)
			fp.Pause(time.Minute)
			done := make(chan struct{})
			go func() {
				defer close(done)
				_, _ = fp.Execute()
			}()

			synctest.Wait()
			//nolint:forbidigo // advances synctest fake clock
			time.Sleep(time.Minute - time.Second)
			synctest.Wait()
			select {
			case <-done:
				s.Fail("pause expired early")
			default:
			}

			//nolint:forbidigo // advances synctest fake clock
			time.Sleep(time.Second)
			synctest.Wait()
			s.Rcv(done)
		})
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
	s.Equal("Panic", failpoint.ActionPanic.String())
	s.Equal("Block", failpoint.ActionBlock.String())
	s.Equal("Pause", failpoint.ActionPause.String())
	s.Equal("Call", failpoint.ActionCall.String())
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
		s.Panics(func() { fp.Block(failpoint.Trigger{MaxFires: -1}) })
		s.Panics(func() { fp.On(failpoint.Trigger{}, failpoint.Trigger{}) })
		s.Equal(failpoint.ActionOff, fp.State().Action, "a rejected setter changes nothing")
	})
}
