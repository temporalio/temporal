//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination rescheduler_mock.go

package queues

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/common/timer"
	"go.temporal.io/server/common/util"
)

const (
	taskChanFullBackoff                  = 2 * time.Second
	taskChanFullBackoffJitterCoefficient = 0.5

	reschedulerPQCleanupDuration          = 3 * time.Minute
	reschedulerPQCleanupJitterCoefficient = 0.15
)

type (
	// Rescheduler buffers task executables that are failed to process and
	// resubmit them to the task scheduler when the Reschedule method is called.
	Rescheduler interface {
		// Add task executable to the rescheduler.
		Add(task Executable, rescheduleTime time.Time)

		// Reschedule triggers an immediate reschedule for provided namespace
		// ignoring executable's reschedule time.
		// Used by namespace failover logic
		Reschedule(namespaceID string)

		// Len returns the total number of task executables waiting to be rescheduled.
		Len() int
		Start()
		Stop()
	}

	rescheduledExecuable struct {
		executable     Executable
		rescheduleTime time.Time
	}

	// reschedulerKey partitions parked tasks by the throttle class that governs them as well as
	// by namespace and priority, so a class that is waiting on a budget cannot head of line
	// block a class that failed for an unrelated reason and is ready to run now. The zero
	// ThrottleKey is what marks a class ungated.
	reschedulerKey struct {
		TaskChannelKey
		Throttle ThrottleKey
	}

	reschedulerImpl struct {
		scheduler                   Scheduler
		timeSource                  clock.TimeSource
		logger                      log.Logger
		metricsHandler              metrics.Handler
		throttleState               ThrottleController
		maxThrottledReleasesPerPass dynamicconfig.IntPropertyFn

		status     int32
		shutdownCh chan struct{}
		shutdownWG sync.WaitGroup

		timerGate        timer.Gate
		taskChannelKeyFn TaskChannelKeyFn

		sync.Mutex
		pqMap          map[reschedulerKey]collection.Queue[rescheduledExecuable]
		keyOrder       []reschedulerKey
		rrCursor       int
		numExecutables int
	}
)

func NewRescheduler(
	scheduler Scheduler,
	timeSource clock.TimeSource,
	logger log.Logger,
	metricsHandler metrics.Handler,
	throttleState ThrottleController,
	maxThrottledReleasesPerPass dynamicconfig.IntPropertyFn,
) *reschedulerImpl {
	if maxThrottledReleasesPerPass == nil {
		maxThrottledReleasesPerPass = dynamicconfig.GetIntPropertyFn(math.MaxInt)
	}
	return &reschedulerImpl{
		scheduler:                   scheduler,
		timeSource:                  timeSource,
		logger:                      logger,
		metricsHandler:              metricsHandler,
		throttleState:               throttleState,
		maxThrottledReleasesPerPass: maxThrottledReleasesPerPass,

		status:     common.DaemonStatusInitialized,
		shutdownCh: make(chan struct{}),

		timerGate:        timer.NewLocalGate(timeSource),
		taskChannelKeyFn: scheduler.TaskChannelKeyFn(),

		pqMap: make(map[reschedulerKey]collection.Queue[rescheduledExecuable]),
	}
}

func (r *reschedulerImpl) Start() {
	if !atomic.CompareAndSwapInt32(&r.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	r.shutdownWG.Add(1)
	go r.rescheduleLoop()

	r.logger.Info("Task rescheduler started.", tag.LifeCycleStarted)
}

func (r *reschedulerImpl) Stop() {
	if !atomic.CompareAndSwapInt32(&r.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	close(r.shutdownCh)
	r.timerGate.Close()

	if success := common.AwaitWaitGroup(&r.shutdownWG, time.Minute); !success {
		r.logger.Warn("Task rescheduler timedout on shutdown.", tag.LifeCycleStopTimedout)
	}

	r.logger.Info("Task rescheduler stopped.", tag.LifeCycleStopped)
}

func (r *reschedulerImpl) Add(
	executable Executable,
	rescheduleTime time.Time,
) {
	key := reschedulerKey{TaskChannelKey: r.taskChannelKeyFn(executable)}
	if r.gating() {
		key.Throttle, _ = executableThrottleKey(executable)
	}

	r.Lock()
	pq := r.getOrCreateClassLocked(key)
	pq.Add(rescheduledExecuable{
		executable:     executable,
		rescheduleTime: rescheduleTime,
	})
	r.numExecutables++
	r.timerGate.Update(rescheduleTime)
	r.Unlock()

	if r.isStopped() {
		r.drain()
	}
}

// executableThrottleKey returns the throttle controller key the executable last failed under.
// Executables that did not fail with a controller relevant resource exhausted error report no
// key, which keeps them in an ungated class.
func executableThrottleKey(executable Executable) (ThrottleKey, bool) {
	reporter, ok := executable.(ThrottleKeyProvider)
	if !ok {
		return ThrottleKey{}, false
	}
	return reporter.ThrottleKey()
}

// setThrottleAdmitted tells the executable whether this dispatch was metered by the controller,
// so a rejection from it is a signal the control law may act on.
func setThrottleAdmitted(executable Executable, key ThrottleKey) {
	if reporter, ok := executable.(ThrottleKeyProvider); ok {
		reporter.SetThrottleAdmitted(key)
	}
}

func (r *reschedulerImpl) Reschedule(
	namespaceID string,
) {
	r.Lock()
	defer r.Unlock()

	now := r.timeSource.Now()
	updatedRescheduleTime := false
	for key, pq := range r.pqMap {
		if key.NamespaceID != namespaceID {
			continue
		}

		updatedRescheduleTime = true
		// set reschedule time for all tasks in this pq to be now
		items := make([]rescheduledExecuable, 0, pq.Len())
		for !pq.IsEmpty() {
			rescheduled := pq.Remove()
			// scheduled queue pre-fetches tasks,
			// so we need to make sure the reschedule time is not before the task scheduled time
			rescheduled.rescheduleTime = util.MaxTime(
				rescheduled.executable.GetKey().FireTime.Add(common.ScheduledTaskMinPrecision),
				now,
			)
			items = append(items, rescheduled)
		}
		r.pqMap[key] = r.newPriorityQueue(items)
	}

	// then update timer gate to trigger the actual reschedule
	if updatedRescheduleTime {
		r.timerGate.Update(now)
	}
}

func (r *reschedulerImpl) Len() int {
	r.Lock()
	defer r.Unlock()

	return r.numExecutables
}

func (r *reschedulerImpl) rescheduleLoop() {
	defer r.shutdownWG.Done()

	cleanupTimer := time.NewTimer(backoff.Jitter(
		reschedulerPQCleanupDuration,
		reschedulerPQCleanupJitterCoefficient,
	))
	defer cleanupTimer.Stop()

	for {
		select {
		case <-r.shutdownCh:
			r.drain()
			return
		case <-r.timerGate.FireCh():
			r.reschedule()
		case <-cleanupTimer.C:
			r.cleanupPQ()
			cleanupTimer.Reset(backoff.Jitter(
				reschedulerPQCleanupDuration,
				reschedulerPQCleanupJitterCoefficient,
			))
		}
	}

}

// reschedulePass is the state one reschedule pass shares across classes: the running minimum
// wake time, and a release ceiling every gated class draws from so one class cannot consume the
// whole pass.
type reschedulePass struct {
	now               time.Time
	nextWake          time.Time
	releasesRemaining int
}

func (p *reschedulePass) wakeAt(t time.Time) {
	if p.nextWake.IsZero() || t.Before(p.nextWake) {
		p.nextWake = t
	}
}

func (r *reschedulerImpl) reschedule() {
	r.Lock()
	defer r.Unlock()

	metrics.TaskReschedulerPendingTasks.With(r.metricsHandler).Record(int64(r.numExecutables))
	now := r.timeSource.Now()

	if !r.gating() {
		r.rescheduleUngatedLocked(now)
		return
	}

	// A non positive cap means unlimited, matching the unset default. Treating 0 as a literal
	// ceiling would make every gated class break before its first release and never drain.
	remaining := r.maxThrottledReleasesPerPass()
	if remaining <= 0 {
		remaining = math.MaxInt
	}
	pass := reschedulePass{now: now, releasesRemaining: remaining}

	n := len(r.keyOrder)
	for i := 0; i < n; i++ {
		key := r.keyOrder[(r.rrCursor+i)%n]
		if pq, ok := r.pqMap[key]; ok && !pq.IsEmpty() {
			r.drainClassLocked(key, pq, &pass)
		}
	}
	if n > 0 {
		r.rrCursor = (r.rrCursor + 1) % n
	}

	if !pass.nextWake.IsZero() {
		r.timerGate.Update(pass.nextWake)
	}
}

// drainClassLocked releases from one class until it runs out of due tasks, of pass budget, or of
// scheduler capacity, recording on pass when the class next wants to be woken.
//
// Budget is a ceiling, never a quota: per task backoff still governs individual eligibility, so a
// class with budget left may still release nothing because its head is not due. The head is never
// reached past, because a class queue is time ordered and shrinkRange derives the ack level from
// the oldest pending key.
func (r *reschedulerImpl) drainClassLocked(
	key reschedulerKey,
	pq collection.Queue[rescheduledExecuable],
	pass *reschedulePass,
) {
	gated := key.Throttle != (ThrottleKey{})
	tags := r.classTags(key)
	metrics.TaskReschedulerClassQueueDepth.With(r.metricsHandler).Record(int64(pq.Len()), tags...)

	for !pq.IsEmpty() {
		if gated && pass.releasesRemaining <= 0 {
			// The pass, not the class, is out of room. Come back promptly, since the work is
			// due and some other class may simply have taken this pass's releases.
			pass.wakeAt(pass.now.Add(r.budgetRetryInterval(0)))
			return
		}

		rescheduled := pq.Peek()
		if rescheduleTime := rescheduled.rescheduleTime; pass.now.Before(rescheduleTime) {
			pass.wakeAt(rescheduleTime)
			return
		}

		executable := rescheduled.executable
		if executable.State() == ctasks.TaskStateCancelled {
			pq.Remove()
			r.numExecutables--
			continue
		}

		metered := false
		if gated {
			var allowed bool
			var retryAfter time.Duration
			allowed, metered, retryAfter = r.throttleState.Admit(key.Throttle)
			if !allowed {
				metrics.TaskReschedulerBudgetDenied.With(r.metricsHandler).Record(1, tags...)
				// The class is over its admitted rate. Come back when the gate expects to have
				// a token, rather than at the head's own backoff, which is far longer.
				pass.wakeAt(pass.now.Add(r.budgetRetryInterval(retryAfter)))
				return
			}
		}

		executable.SetScheduledTime(pass.now)
		if metered {
			// Mark before submitting. TrySubmit hands the executable to a worker that can reach
			// HandleErr before this goroutine continues, and a rejection the gate is not
			// recorded as having issued is discarded by the control law. Only a metered release
			// is marked: past the key cap the gate admits without tracking anything.
			setThrottleAdmitted(executable, key.Throttle)
		}
		if !r.scheduler.TrySubmit(executable) {
			if metered {
				setThrottleAdmitted(executable, ThrottleKey{})
				r.throttleState.Return(key.Throttle)
			}
			pass.wakeAt(pass.now.Add(
				backoff.Jitter(taskChanFullBackoff, taskChanFullBackoffJitterCoefficient)))
			return
		}

		pq.Remove()
		r.numExecutables--
		metrics.TaskReschedulerReleases.With(r.metricsHandler).Record(1, tags...)
		if gated {
			pass.releasesRemaining--
		}
	}
}

// gating reports whether the throttle controller is governing releases. When it is not, the
// rescheduler runs exactly as it does upstream: one queue per task channel key, no round robin
// cursor, and the timer gate updated inline from whichever class set it last.
func (r *reschedulerImpl) gating() bool {
	return r.throttleState != nil && r.throttleState.Enabled()
}

// rescheduleUngatedLocked is the upstream reschedule loop, kept verbatim so that disabling the
// controller disables the whole change and not just the admission gate.
func (r *reschedulerImpl) rescheduleUngatedLocked(now time.Time) {
	for _, pq := range r.pqMap {
		for !pq.IsEmpty() {
			rescheduled := pq.Peek()

			if rescheduleTime := rescheduled.rescheduleTime; now.Before(rescheduleTime) {
				r.timerGate.Update(rescheduleTime)
				break
			}

			executable := rescheduled.executable
			if executable.State() == ctasks.TaskStateCancelled {
				pq.Remove()
				r.numExecutables--
				continue
			}

			executable.SetScheduledTime(now)
			if !r.scheduler.TrySubmit(executable) {
				r.timerGate.Update(now.Add(backoff.Jitter(taskChanFullBackoff, taskChanFullBackoffJitterCoefficient)))
				break
			}

			pq.Remove()
			r.numExecutables--
		}
	}
}

// budgetRetryInterval is how long a budget denied class waits before retrying.
//
// eta can only push the wait out, never pull it in. The bucket is shared by every shard's
// rescheduler on the host, so a per-shard estimate assumes a consumer it does not have; honouring
// a shorter one would make each shard poll at the whole class's refill rate. Capped at one window
// because the rate moves there.
func (r *reschedulerImpl) budgetRetryInterval(eta time.Duration) time.Duration {
	const budgetRetryDivisor = 10

	window := r.throttleState.Window()
	interval := min(max(eta, window/budgetRetryDivisor), window)
	return max(interval, time.Millisecond)
}

func (r *reschedulerImpl) classTags(key reschedulerKey) []metrics.Tag {
	return []metrics.Tag{
		metrics.NamespaceIDTag(key.NamespaceID),
		metrics.TaskPriorityTag(key.Priority.String()),
		metrics.ResourceExhaustedCauseTag(key.Throttle.Cause),
	}
}

func (r *reschedulerImpl) cleanupPQ() {
	r.Lock()
	defer r.Unlock()

	for key, pq := range r.pqMap {
		if pq.IsEmpty() {
			delete(r.pqMap, key)
		}
	}
	r.rebuildKeyOrderLocked()
}

func (r *reschedulerImpl) drain() {
	r.Lock()
	defer r.Unlock()

	for key, pq := range r.pqMap {
		for !pq.IsEmpty() {
			pq.Remove()
		}
		delete(r.pqMap, key)
	}
	r.keyOrder = nil
	r.rrCursor = 0

	r.numExecutables = 0
}

func (r *reschedulerImpl) rebuildKeyOrderLocked() {
	if len(r.keyOrder) == len(r.pqMap) {
		return
	}
	order := r.keyOrder[:0]
	for _, key := range r.keyOrder {
		if _, ok := r.pqMap[key]; ok {
			order = append(order, key)
		}
	}
	r.keyOrder = order
	if len(r.keyOrder) == 0 {
		r.rrCursor = 0
	}
}

func (r *reschedulerImpl) isStopped() bool {
	return atomic.LoadInt32(&r.status) == common.DaemonStatusStopped
}

func (r *reschedulerImpl) getOrCreateClassLocked(
	key reschedulerKey,
) collection.Queue[rescheduledExecuable] {
	if pq, ok := r.pqMap[key]; ok {
		return pq
	}

	pq := r.newPriorityQueue(nil)
	r.pqMap[key] = pq
	r.keyOrder = append(r.keyOrder, key)
	return pq
}

func (r *reschedulerImpl) newPriorityQueue(
	items []rescheduledExecuable,
) collection.Queue[rescheduledExecuable] {
	if items == nil {
		return collection.NewPriorityQueue(r.rescheduledExecuableCompareLess)
	}

	return collection.NewPriorityQueueWithItems(r.rescheduledExecuableCompareLess, items)
}

func (r *reschedulerImpl) rescheduledExecuableCompareLess(
	this rescheduledExecuable,
	that rescheduledExecuable,
) bool {
	return this.rescheduleTime.Before(that.rescheduleTime)
}
