package stream_batcher

import (
	"context"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/clock"
)

// Batcher collects items concurrently passed to Add into batches and calls a processing
// function on them a batch at a time.
// The processing function will be called on batches of items in a single-threaded manner, and
// Add will block while fn is running.
type Batcher[T, R any] struct {
	fn         batchFn[T, R]        // batch executor function
	opts       BatcherOptions       // timing/size options
	timeSource clock.TimeSource     // clock for testing
	submitC    chan batchPair[T, R] // channel for submitting items
	// keeps track of goroutine state:
	// if goroutine is not running, running == nil.
	// if it is running, running points to a channel that will be closed when the goroutine exits.
	running atomic.Pointer[chan struct{}]
}

type batchPair[T, R any] struct {
	resp chan R // response channel
	item T      // item to add
}

type batchFn[T, R any] func([]T) batchResult[R]

type batchResult[R any] struct {
	shared  R
	perItem []R
}

type BatcherOptions struct {
	// MaxItems is the maximum number of items in a batch.
	MaxItems int
	// MinDelay is how long to wait for no more items to come in after any item before
	// finishing the batch.
	MinDelay time.Duration
	// MaxDelay is the maximum time to wait after the first item in a batch before finishing
	// the batch.
	MaxDelay time.Duration
	// IdleTime is the time after which the internal goroutine will exit, to avoid wasting
	// resources on idle streams.
	IdleTime time.Duration
	// ClearInterval controls how often a KeyedBatcher clears its cached batchers.
	// Zero disables clearing. This option is ignored by Batcher.
	ClearInterval time.Duration
}

// NewBatcher creates a Batcher. `fn` is the processing function, `opts` are the timing options.
// `clock` is usually clock.NewRealTimeSource but can be a fake time source for testing.
func NewBatcher[T, R any](fn func([]T) R, opts BatcherOptions, timeSource clock.TimeSource) *Batcher[T, R] {
	return newBatcher(func(items []T) batchResult[R] {
		return batchResult[R]{shared: fn(items)}
	}, opts, timeSource)
}

// NewBatcherWithPerItemResults creates a Batcher whose processing function returns one result
// for each input item, in the same order. If the processing function returns a different number
// of results, each caller receives the zero value of R.
func NewBatcherWithPerItemResults[T, R any](
	fn func([]T) []R,
	opts BatcherOptions,
	timeSource clock.TimeSource,
) *Batcher[T, R] {
	return newBatcher(func(items []T) batchResult[R] {
		return batchResult[R]{perItem: fn(items)}
	}, opts, timeSource)
}

func newBatcher[T, R any](fn batchFn[T, R], opts BatcherOptions, timeSource clock.TimeSource) *Batcher[T, R] {
	return &Batcher[T, R]{
		fn:         fn,
		opts:       opts,
		timeSource: timeSource,
		submitC:    make(chan batchPair[T, R]),
	}
}

// Add adds an item to the stream and returns when it has been processed, or if the context is
// canceled or times out. It returns the shared batch result or the result corresponding to this
// item, depending on which constructor was used. Even if Add returns a context error, the item
// may still be processed in the future.
func (b *Batcher[T, R]) Add(ctx context.Context, t T) (R, error) {
	resp := make(chan R, 1)
	pair := batchPair[T, R]{resp: resp, item: t}

	for {
		runningC := b.running.Load()
		for runningC == nil {
			// goroutine is not running, try to start it
			newRunningC := make(chan struct{})
			if b.running.CompareAndSwap(nil, &newRunningC) {
				// we were the first one to notice the nil, start it now
				go b.loop(&newRunningC)
			}
			// if CompareAndSwap failed, someone else was calling Add at the same time and
			// started the goroutine already. reload to get the new running channel.
			runningC = b.running.Load()
		}

		select {
		case <-(*runningC):
			// we loaded a non-nil running channel, but it closed while we're waiting to
			// submit. the goroutine must have just exited. try again.
			continue
		case b.submitC <- pair:
			select {
			case r := <-resp:
				return r, nil
			case <-ctx.Done():
				var zeroR R
				return zeroR, ctx.Err()
			}
		case <-ctx.Done():
			var zeroR R
			return zeroR, ctx.Err()
		}
	}
}

func (b *Batcher[T, R]) loop(runningC *chan struct{}) {
	defer func() {
		// store nil so that Add knows it should start a goroutine
		b.running.Store(nil)
		// if Add loaded s.running after we decided to stop but before we Stored nil, so it
		// thought we were running when we're not, then we need to wake it up so that it can
		// start us again.
		close(*runningC)
	}()

	var items []T
	var resps []chan R
	for {
		clear(items)
		clear(resps)
		items, resps = items[:0], resps[:0]

		// wait for first item. if no item after a while, exit the goroutine
		idleC, idleT := b.timeSource.NewTimer(b.opts.IdleTime)
		select {
		case pair := <-b.submitC:
			items = append(items, pair.item)
			resps = append(resps, pair.resp)
		case <-idleC:
			return
		}
		idleT.Stop()

		// try to add more items. stop after a gap of MinDelay, total time of MaxDelay,
		// or MaxItems items.
		maxWaitC, maxWaitT := b.timeSource.NewTimer(b.opts.MaxDelay)
	loop:
		for len(items) < b.opts.MaxItems {
			gapC, gapT := b.timeSource.NewTimer(b.opts.MinDelay)
			select {
			case pair := <-b.submitC:
				items = append(items, pair.item)
				resps = append(resps, pair.resp)
			case <-gapC:
				break loop
			case <-maxWaitC:
				gapT.Stop()
				break loop
			}
			gapT.Stop()
		}
		maxWaitT.Stop()

		// process batch
		results := b.fn(items)
		validPerItemResults := len(results.perItem) == len(items)

		for i, resp := range resps {
			if results.perItem != nil && validPerItemResults {
				resp <- results.perItem[i]
			} else if results.perItem != nil {
				var zeroR R
				resp <- zeroR
			} else {
				resp <- results.shared
			}
		}
	}
}
