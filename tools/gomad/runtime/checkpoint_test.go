package sim_runtime_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	SIM "go.temporal.io/server/tools/gomad/runtime"
)

func TestCheckpoint_BasicRecordAndTake(t *testing.T) {
	SIM.Start(SIM.Seed(42))
	SIM.StartRecording()

	var result int64
	SIM.NewGoroutine(func() {
		result = SIM.CurrentSimulator().Drng.Int63n(1000)
	}, false)

	var cp *SIM.Checkpoint
	SIM.NewGoroutine(func() {
		cp = SIM.TakeCheckpoint()
	}, false)

	SIM.Join()

	require.NotNil(t, cp)
	require.NotZero(t, result)
}

func TestCheckpoint_ChannelRecordAndReplay(t *testing.T) {
	SIM.Start(SIM.Seed(42))
	SIM.StartRecording()

	ch := make(chan int, 1)
	var received int

	SIM.NewGoroutine(func() {
		simCh := SIM.GetOrCreateChan(ch)
		simCh.Snd(42)
	}, false)

	SIM.NewGoroutine(func() {
		simCh := SIM.GetOrCreateChan(ch)
		msg, ok := simCh.RcvOk()
		require.True(t, ok)
		received = msg.(int)
	}, false)

	SIM.Join()

	require.Equal(t, 42, received)
}

func TestCheckpoint_SleepRecord(t *testing.T) {
	SIM.Start(SIM.Seed(42))
	SIM.StartRecording()

	var slept bool
	SIM.NewGoroutine(func() {
		SIM.Sleep(100 * time.Millisecond)
		slept = true
	}, false)

	SIM.Join()

	require.True(t, slept)
}

func TestCheckpoint_RestoreAndContinue(t *testing.T) {
	// Phase 1: run simulation, take checkpoint mid-way
	SIM.Start(SIM.Seed(42))
	SIM.StartRecording()

	var counter atomic.Int64
	var cp *SIM.Checkpoint

	SIM.NewGoroutine(func() {
		counter.Add(1) // step 1
		SIM.Sleep(10 * time.Millisecond)
		counter.Add(1) // step 2
	}, false)

	SIM.NewGoroutine(func() {
		// take checkpoint after a short delay
		SIM.Sleep(5 * time.Millisecond)
		cp = SIM.TakeCheckpoint()
	}, false)

	SIM.Join()

	require.NotNil(t, cp)
	val1 := counter.Load()
	require.Equal(t, int64(2), val1)

	// Phase 2: restore from checkpoint and run again
	counter.Store(0)
	SIM.RestoreFrom(cp)

	// after restore, goroutines replay their logs then continue normally
	SIM.Join()

	val2 := counter.Load()
	require.Equal(t, int64(2), val2)
}

func TestCheckpoint_DrngDeterminism(t *testing.T) {
	// Take a checkpoint via a goroutine that only does deterministic DRNG work,
	// restore twice, and verify the post-restore DRNG produces identical values.
	SIM.Start(SIM.Seed(99))
	SIM.StartRecording()

	// use a separate var so the replayed goroutine's overwrite doesn't affect saved
	var latestCp *SIM.Checkpoint
	SIM.NewGoroutine(func() {
		latestCp = SIM.TakeCheckpoint()
	}, false)

	SIM.Join()
	require.NotNil(t, latestCp)
	savedCp := latestCp // save before restore overwrites it

	// first restore
	var result1 int64
	SIM.RestoreFrom(savedCp)
	SIM.NewGoroutine(func() {
		result1 = SIM.CurrentSimulator().Drng.Int63n(10000)
	}, false)
	SIM.Join()

	// second restore from the same saved checkpoint
	var result2 int64
	SIM.RestoreFrom(savedCp)
	SIM.NewGoroutine(func() {
		result2 = SIM.CurrentSimulator().Drng.Int63n(10000)
	}, false)
	SIM.Join()

	require.Equal(t, result1, result2, "DRNG should be deterministic after restore")
}

func TestCheckpoint_RestoreBranching(t *testing.T) {
	// Take a checkpoint, then branch into two different continuations
	// and verify they produce identical DRNG sequences.
	SIM.Start(SIM.Seed(7))
	SIM.StartRecording()

	var savedCp *SIM.Checkpoint
	SIM.NewGoroutine(func() {
		// do some deterministic work before checkpointing
		SIM.CurrentSimulator().Drng.Int63n(100)
		savedCp = SIM.TakeCheckpoint()
	}, false)

	SIM.Join()
	require.NotNil(t, savedCp)
	cpCopy := savedCp

	// Branch A
	var branchAResult int64
	SIM.RestoreFrom(cpCopy)
	SIM.NewGoroutine(func() {
		branchAResult = SIM.CurrentSimulator().Drng.Int63n(10000)
	}, false)
	SIM.Join()

	// Branch B (from same checkpoint)
	var branchBResult int64
	SIM.RestoreFrom(cpCopy)
	SIM.NewGoroutine(func() {
		branchBResult = SIM.CurrentSimulator().Drng.Int63n(10000)
	}, false)
	SIM.Join()

	require.Equal(t, branchAResult, branchBResult, "branches from same checkpoint must be deterministic")
}

func TestCheckpoint_ChannelBufferRestore(t *testing.T) {
	// Verify channel buffer state is correctly restored.
	SIM.Start(SIM.Seed(42))
	SIM.StartRecording()

	ch := make(chan int, 5)
	var savedCp *SIM.Checkpoint

	// single goroutine: send values, take checkpoint
	SIM.NewGoroutine(func() {
		simCh := SIM.GetOrCreateChan(ch)
		simCh.Snd(10)
		simCh.Snd(20)
		simCh.Snd(30)
		savedCp = SIM.TakeCheckpoint()
	}, false)

	SIM.Join()
	require.NotNil(t, savedCp)
	cpCopy := savedCp

	// restore and read from the buffer — should get the first buffered value
	var received int
	SIM.RestoreFrom(cpCopy)
	SIM.NewGoroutine(func() {
		simCh := SIM.GetOrCreateChan(ch)
		msg, _ := simCh.RcvOk()
		received = msg.(int)
	}, false)
	SIM.Join()

	require.Equal(t, 10, received, "first buffered message should be 10")
}
