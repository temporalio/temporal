package stream_batcher

import (
	"context"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common/clock"
)

// this test uses time.Sleep to allow goroutines to get into a blocked state
//
//nolint:forbidigo
func TestStreamBatcher_MinDelay(t *testing.T) {
	clk := clock.NewEventTimeSource()

	opts := BatcherOptions{
		MaxItems: 10,
		MinDelay: 100 * time.Millisecond,
		MaxDelay: 400 * time.Millisecond,
		IdleTime: 1000 * time.Millisecond,
	}
	process := func(items []int) (total int) {
		for _, i := range items {
			total += i
		}
		clk.Sleep(50 * time.Millisecond)
		return
	}
	sb := NewBatcher(process, opts, clk)

	var wg sync.WaitGroup
	wg.Add(3)

	// 350 = 200 (last Add call) + 100 (MinDelay) + 50 (process time)
	targetMS := int64(350)

	go func() {
		defer wg.Done()
		clk.Sleep(100 * time.Millisecond)
		ctx := context.Background()
		total, err := sb.Add(ctx, 100)
		assert.NoError(t, err)
		assert.Equal(t, 123, total)
		assert.Equal(t, targetMS, clk.Now().UnixMilli())
	}()
	go func() {
		defer wg.Done()
		clk.Sleep(150 * time.Millisecond)
		ctx := context.Background()
		total, err := sb.Add(ctx, 20)
		assert.NoError(t, err)
		assert.Equal(t, 123, total)
		assert.Equal(t, targetMS, clk.Now().UnixMilli())
	}()
	go func() {
		defer wg.Done()
		clk.Sleep(200 * time.Millisecond)
		ctx := context.Background()
		total, err := sb.Add(ctx, 3)
		assert.NoError(t, err)
		assert.Equal(t, 123, total)
		assert.Equal(t, targetMS, clk.Now().UnixMilli())
	}()

	time.Sleep(time.Millisecond) // wait for goroutines to get into Add
	clk.AdvanceNext()            // first Add
	time.Sleep(time.Millisecond)
	clk.AdvanceNext() // second Add
	time.Sleep(time.Millisecond)
	clk.AdvanceNext() // third add
	time.Sleep(time.Millisecond)
	clk.AdvanceNext() // min delay
	time.Sleep(time.Millisecond)
	clk.AdvanceNext() // process time

	wg.Wait()
}

// this test uses time.Sleep to allow goroutines to get into a blocked state
//
//nolint:forbidigo
func TestStreamBatcher_MaxDelay(t *testing.T) {
	clk := clock.NewEventTimeSource()

	opts := BatcherOptions{
		MaxItems: 10,
		MinDelay: 100 * time.Millisecond,
		MaxDelay: 120 * time.Millisecond,
		IdleTime: 1000 * time.Millisecond,
	}
	process := func(items []int) (total int) {
		for _, i := range items {
			total += i
		}
		clk.Sleep(50 * time.Millisecond)
		return
	}
	sb := NewBatcher(process, opts, clk)

	var wg sync.WaitGroup
	wg.Add(4)

	// 270 = 220 (first Add call + max delay) + 50 (process time)
	targetMS := int64(270)

	go func() {
		defer wg.Done()
		clk.Sleep(100 * time.Millisecond)
		ctx := context.Background()
		total, err := sb.Add(ctx, 100)
		assert.NoError(t, err)
		assert.Equal(t, 123, total)
		assert.Equal(t, targetMS, clk.Now().UnixMilli())
	}()
	go func() {
		defer wg.Done()
		clk.Sleep(150 * time.Millisecond)
		ctx := context.Background()
		total, err := sb.Add(ctx, 20)
		assert.NoError(t, err)
		assert.Equal(t, 123, total)
		assert.Equal(t, targetMS, clk.Now().UnixMilli())
	}()
	go func() {
		defer wg.Done()
		clk.Sleep(200 * time.Millisecond)
		ctx := context.Background()
		total, err := sb.Add(ctx, 3)
		assert.NoError(t, err)
		assert.Equal(t, 123, total)
		assert.Equal(t, targetMS, clk.Now().UnixMilli())
	}()
	go func() {
		defer wg.Done()
		clk.Sleep(250 * time.Millisecond)
		ctx := context.Background()
		// misses the first batch, will be in separate batch
		total, err := sb.Add(ctx, 777)
		assert.NoError(t, err)
		assert.Equal(t, 777, total)
		// end of first process time + 100 (min delay) + 50 (second process time)
		assert.Equal(t, targetMS+100+50, clk.Now().UnixMilli())
	}()

	time.Sleep(time.Millisecond) // wait for goroutines to get into Add
	clk.AdvanceNext()            // first Add
	time.Sleep(time.Millisecond)
	clk.AdvanceNext() // second Add
	time.Sleep(time.Millisecond)
	clk.AdvanceNext() // third add
	time.Sleep(time.Millisecond)
	clk.AdvanceNext() // max delay
	time.Sleep(time.Millisecond)
	clk.AdvanceNext() // process time
	time.Sleep(time.Millisecond)
	clk.AdvanceNext() // fourth add
	time.Sleep(time.Millisecond)
	clk.AdvanceNext() // min delay
	time.Sleep(time.Millisecond)
	clk.AdvanceNext() // process time

	wg.Wait()
}

// this test uses time.Sleep to allow goroutines to get into a blocked state
//
//nolint:forbidigo
func TestStreamBatcher_MaxItems(t *testing.T) {
	clk := clock.NewEventTimeSource()

	opts := BatcherOptions{
		MaxItems: 2,
		MinDelay: 100 * time.Millisecond,
		MaxDelay: 400 * time.Millisecond,
		IdleTime: 1000 * time.Millisecond,
	}
	process := func(items []int) (total int) {
		for _, i := range items {
			total += i
		}
		clk.Sleep(50 * time.Millisecond)
		return
	}
	sb := NewBatcher(process, opts, clk)

	var wg sync.WaitGroup
	wg.Add(2)

	// 200 = 150 (second Add call) + 50 (process time)
	targetMS := int64(200)

	go func() {
		defer wg.Done()
		clk.Sleep(100 * time.Millisecond)
		ctx := context.Background()
		total, err := sb.Add(ctx, 100)
		assert.NoError(t, err)
		assert.Equal(t, 123, total)
		assert.Equal(t, targetMS, clk.Now().UnixMilli())
	}()
	go func() {
		defer wg.Done()
		clk.Sleep(150 * time.Millisecond)
		ctx := context.Background()
		total, err := sb.Add(ctx, 23)
		assert.NoError(t, err)
		assert.Equal(t, 123, total)
		assert.Equal(t, targetMS, clk.Now().UnixMilli())
	}()

	time.Sleep(time.Millisecond) // wait for goroutines to get into Add
	clk.AdvanceNext()            // first Add
	time.Sleep(time.Millisecond)
	clk.AdvanceNext() // second Add
	time.Sleep(time.Millisecond)
	clk.AdvanceNext() // process time

	wg.Wait()
}

// this test uses time.Sleep to allow goroutines to get into a blocked state
//
//nolint:forbidigo
func TestStreamBatcher_AddTimeout(t *testing.T) {
	clk := clock.NewEventTimeSource()

	opts := BatcherOptions{
		MaxItems: 2,
		MinDelay: 100 * time.Millisecond,
		MaxDelay: 400 * time.Millisecond,
		IdleTime: 1000 * time.Millisecond,
	}
	process := func(items []int) (total int) {
		for _, i := range items {
			total += i
		}
		clk.Sleep(5 * time.Second)
		return
	}
	sb := NewBatcher(process, opts, clk)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// this will time out before a response
		ctx, cancel := clock.ContextWithTimeout(context.Background(), time.Second, clk)
		defer cancel()
		_, err := sb.Add(ctx, 123)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	}()
	time.Sleep(time.Millisecond) // wait for it to block in Add
	clk.AdvanceNext()
	time.Sleep(time.Millisecond)
	clk.AdvanceNext()
	time.Sleep(time.Millisecond)
	clk.AdvanceNext()
	wg.Wait()

	// we should be able to process another batch again
	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx, cancel := clock.ContextWithTimeout(context.Background(), 7*time.Second, clk)
		defer cancel()
		r, err := sb.Add(ctx, 123)
		assert.NoError(t, err)
		assert.Equal(t, r, 123)
	}()
	time.Sleep(time.Millisecond) // wait for it to block in Add
	clk.AdvanceNext()
	time.Sleep(time.Millisecond)
	clk.AdvanceNext()
	wg.Wait()
}

func TestStreamBatcher_Random(t *testing.T) {
	// throw a lot of concurrent calls at the batcher and make sure there are no errors at
	// least. with log statements in stream_batcher.go, you can see this does (or did at some
	// point) exercise all of the tricky spots.

	clk := clock.NewEventTimeSource()

	const workers = 20
	const events = 1000

	opts := BatcherOptions{
		MaxItems: 10,
		MinDelay: 100 * time.Millisecond,
		MaxDelay: 400 * time.Millisecond,
		IdleTime: 100 * time.Millisecond,
	}
	process := func(items []int) (total int) {
		for _, i := range items {
			total += i
		}
		clk.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
		return
	}
	sb := NewBatcher(process, opts, clk)

	var running atomic.Int64
	for range workers {
		running.Add(1)
		go func() {
			defer running.Add(-1)
			for range events {
				// 600ms is tuned to create a small-ish number of timeouts given the constants above
				ctx, cancel := clock.ContextWithTimeout(context.Background(), 600*time.Millisecond, clk)
				_, _ = sb.Add(ctx, 1)
				cancel()
				clk.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
			}
		}()
	}

	for running.Load() > 0 {
		clk.AdvanceNext()
		// What we'd really like is a way to say "wait until all of these goroutines are
		// blocked on a timer". But that's not quite possible with the Go runtime. Running
		// Gosched repeatedly is good enough for this test.
		for range workers + 1 {
			runtime.Gosched()
		}
	}
}
