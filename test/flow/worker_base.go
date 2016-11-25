package flow

import (
	"sync"
	"time"

	m "code.uber.internal/devexp/minions/.gen/go/minions"
	"code.uber.internal/devexp/minions/common"
	"code.uber.internal/devexp/minions/common/backoff"
	log "github.com/Sirupsen/logrus"
)

const (
	tagWorkerID  = "WorkerID"
	tagRoutineID = "routineID"

	retryPollOperationInitialInterval    = time.Millisecond
	retryPollOperationMaxInterval        = 1 * time.Second
	retryPollOperationExpirationInterval = backoff.NoInterval // We don't ever expire
)

var (
	pollOperationRetryPolicy = createPollRetryPolicy()
)

type (
	// baseWorkerOptions options to configure base worker.
	baseWorkerOptions struct {
		routineCount    int
		taskPoller      TaskPoller
		workflowService m.TChanWorkflowService
		identity        string
	}

	// baseWorker that wraps worker activities.
	baseWorker struct {
		options         baseWorkerOptions
		isWorkerStarted bool
		shutdownCh      chan struct{}              // Channel used to shut down the go routines.
		shutdownWG      sync.WaitGroup             // The WaitGroup for shutting down existing routines.
		rateLimiter     common.TokenBucket         // Poll rate limiter
		retrier         *backoff.ConcurrentRetrier // Service errors back off retrier
	}
)

func createPollRetryPolicy() backoff.RetryPolicy {
	policy := backoff.NewExponentialRetryPolicy(retryPollOperationInitialInterval)
	policy.SetMaximumInterval(retryPollOperationMaxInterval)
	policy.SetExpirationInterval(retryPollOperationExpirationInterval)
	return policy
}

func newBaseWorker(options baseWorkerOptions) *baseWorker {
	return &baseWorker{
		options:     options,
		shutdownCh:  make(chan struct{}),
		rateLimiter: common.NewTokenBucket(1000, common.NewRealTimeSource()),
		retrier:     backoff.NewConcurrentRetrier(pollOperationRetryPolicy)}
}

// Start starts a fixed set of routines to do the work.
func (bw *baseWorker) Start() {
	if bw.isWorkerStarted {
		return
	}
	// Add the total number of routines to the wait group
	bw.shutdownWG.Add(bw.options.routineCount)

	// Launch the routines to do work
	for i := 0; i < bw.options.routineCount; i++ {
		go bw.execute(i)
	}

	bw.isWorkerStarted = true
}

// Shutdown is a blocking call and cleans up all the resources assosciated with worker.
func (bw *baseWorker) Shutdown() {
	if !bw.isWorkerStarted {
		return
	}
	close(bw.shutdownCh)
	// TODO: This needs to have a time out that worker routines in-definitely doesn't block us,
	// also need a way to preempt the go routines so we don't hold on the resources after this.
	bw.shutdownWG.Wait()
}

// execute handler wraps call to process a task.
func (bw *baseWorker) execute(routineID int) {
	for {
		// Check if we have to backoff.
		// TODO: Check if this is needed concurrent retires (or) per connection retrier.
		bw.retrier.Throttle()

		// Check if we are rate limited
		if !bw.rateLimiter.Consume(1, time.Millisecond) {
			continue
		}

		err := bw.options.taskPoller.PollAndProcessSingleTask()
		if err != nil {
			log.WithFields(log.Fields{tagWorkerID: bw.options.identity, tagRoutineID: routineID}).Error("Poll failed with error:", err)
			bw.retrier.Failed()
		} else {
			bw.retrier.Succeeded()
		}

		select {
		// Shutdown the Routine.
		case <-bw.shutdownCh:
			log.WithFields(log.Fields{tagWorkerID: bw.options.identity, tagRoutineID: routineID}).Debug("Shutting Down!")
			bw.shutdownWG.Done()
			return

		// We have work to do.
		default:
		}
	}
}
