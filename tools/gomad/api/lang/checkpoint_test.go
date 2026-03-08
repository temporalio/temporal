package lang_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	SIMLANG "go.temporal.io/server/tools/gomad/api/lang"
	SIM "go.temporal.io/server/tools/gomad/runtime"
	"go.temporal.io/server/tools/gomad/runtime/testutil"
)

func TestCheckpointReplay(t *testing.T) {

	t.Run("ping-pong replay produces identical results", func(t *testing.T) {
		for i := int64(0); i < testutil.TestRuns; i++ {
			var res1, res2 []string

			run := func() ([]string, *SIM.Checkpoint) {
				var res []string
				var cp *SIM.Checkpoint
				ch := make(chan string)

				SIMLANG.Go(func() {
					SIMLANG.ChanSend(ch, "ping")
					res = append(res, SIMLANG.ChanRcv(ch))
					SIMLANG.ChanSend(ch, "ping")
					res = append(res, SIMLANG.ChanRcv(ch))
				})

				SIMLANG.Go(func() {
					res = append(res, SIMLANG.ChanRcv(ch))
					SIMLANG.ChanSend(ch, "pong")
					cp = SIM.TakeCheckpoint()
					res = append(res, SIMLANG.ChanRcv(ch))
					SIMLANG.ChanSend(ch, "pong")
				})

				SIM.Join()
				return res, cp
			}

			// phase 1: record
			testutil.SingleRun(func(seed int64) {
				SIM.StartRecording()
				res1, _ = run()
			}, i)

			// phase 2: fresh run with same seed — must match
			testutil.SingleRun(func(seed int64) {
				SIM.StartRecording()
				res2, _ = run()
			}, i)

			require.Equal(t, res1, res2, "seed %d: results must be deterministic", i)
		}
	})

	t.Run("checkpoint and restore with channels", func(t *testing.T) {
		for i := int64(0); i < testutil.TestRuns; i++ {
			var savedCp *SIM.Checkpoint
			var resultA, resultB int64

			// phase 1: run and take checkpoint
			testutil.SingleRun(func(seed int64) {
				SIM.StartRecording()

				ch := make(chan int, 10)

				SIMLANG.Go(func() {
					for j := 0; j < 5; j++ {
						SIMLANG.ChanSend(ch, j)
					}
					savedCp = SIM.TakeCheckpoint()
				})

				SIM.Join()
			}, i)

			require.NotNil(t, savedCp, "seed %d: checkpoint must be taken", i)
			cpCopy := savedCp

			// phase 2: restore branch A
			testutil.SingleRun(func(seed int64) {
				// don't call Start — RestoreFrom creates a fresh simulator
			}, i)
			// we need to use RestoreFrom outside SingleRun since it creates its own sim
			func() {
				done := make(chan struct{})
				go func() {
					defer close(done)
					SIM.RestoreFrom(cpCopy)
					SIM.NewGoroutine(func() {
						resultA = SIM.CurrentSimulator().Drng.Int63n(10000)
					}, false)
					SIM.Join()
				}()
				<-done
			}()

			// phase 3: restore branch B — must match A
			func() {
				done := make(chan struct{})
				go func() {
					defer close(done)
					SIM.RestoreFrom(cpCopy)
					SIM.NewGoroutine(func() {
						resultB = SIM.CurrentSimulator().Drng.Int63n(10000)
					}, false)
					SIM.Join()
				}()
				<-done
			}()

			require.Equal(t, resultA, resultB,
				"seed %d: branches from same checkpoint must produce identical results", i)
		}
	})

	t.Run("sleep and select replay", func(t *testing.T) {
		for i := int64(0); i < testutil.TestRuns; i++ {
			var res1, res2 []int

			run := func() []int {
				var res []int
				ch1 := make(chan int, 1)
				ch2 := make(chan int, 1)

				SIMLANG.Go(func() {
					SIM.Sleep(10 * time.Millisecond)
					SIMLANG.ChanSend(ch1, 1)
				})

				SIMLANG.Go(func() {
					SIM.Sleep(20 * time.Millisecond)
					SIMLANG.ChanSend(ch2, 2)
				})

				SIMLANG.Go(func() {
					for j := 0; j < 2; j++ {
						sel := SIMLANG.Select(
							0, SIMLANG.RcvChan(ch1), nil,
							0, SIMLANG.RcvChan(ch2), nil,
						)
						res = append(res, sel.Value.(int))
					}
				})

				SIM.Join()
				return res
			}

			testutil.SingleRun(func(seed int64) {
				SIM.StartRecording()
				res1 = run()
			}, i)

			testutil.SingleRun(func(seed int64) {
				SIM.StartRecording()
				res2 = run()
			}, i)

			require.Equal(t, res1, res2, "seed %d: select results must be deterministic", i)
		}
	})
}
