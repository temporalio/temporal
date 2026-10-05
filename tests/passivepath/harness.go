// Package passivepath provides a test harness that makes a single-cluster Temporal
// server persist workflow state exclusively through the *passive* replication apply
// path, so that passive task-refresh logic can be exercised by ordinary workflows.
//
// Normally an active mutation is committed by workflow.ContextImpl:
// CloseTransactionAsMutation -> UpdateWorkflowExecution, which writes both the mutable
// state and the active-generated transfer/timer tasks. This harness intercepts that
// commit, converts the just-closed mutable state into a VersionedTransitionArtifact,
// discards the active write, and applies the artifact via
// Engine.ReplicateVersionedTransition. The passive apply path then writes the mutable
// state together with tasks produced by workflow.TaskRefresher.
//
// Because there is exactly one cluster and it is active for the namespace, the
// harness can also exercise each refresher-generated task in standby mode before
// its normal active execution. ErrTaskRetry hands the task to the active executor;
// a nil standby result acknowledges it without active execution. If standby wrongly
// drops a task that the workflow needs, the workflow stalls and the test catches it.
package passivepath

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"

	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/serialization"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/replication"
	historytasks "go.temporal.io/server/service/history/tasks"
)

// BailReason records why a write was not diverted through the passive path.
type BailReason string

const (
	// BailPassivePolicy is a non-active transaction, including the replicator's own
	// passive write. Expected and required: diverting it would recurse forever.
	BailPassivePolicy BailReason = "passive-policy"
	// BailNewRun is an incomplete or non-active update-with-new request. Supported
	// active continue-as-new requests are diverted together with their successor run.
	BailNewRun BailReason = "new-run"
	// BailUpdateMode is an update mode other than UpdateCurrent (e.g. zombie workflows).
	BailUpdateMode BailReason = "update-mode"
	// BailNoTransitionHistory means transition history is empty, so there is nothing to
	// anchor an artifact to. Requires dynamicconfig.EnableTransitionHistory.
	BailNoTransitionHistory BailReason = "no-transition-history"
	// BailBufferedEvents means the mutable state has buffered events. These cannot be
	// represented in an artifact at all -- neither WorkflowMutableState nor
	// WorkflowMutableStateMutation has a buffered_events field -- which is why the real
	// SyncStateRetriever rejects them outright. They are a transient active-side staging
	// area that gets folded into a real history batch at the next workflow task, after
	// which they replicate normally. Real XDC waits for that flush; so do we.
	BailBufferedEvents BailReason = "buffered-events"
	// BailClearBufferedEvents persists the transaction that removes buffered events
	// written by an earlier active bailout. The replication mutation does not carry
	// this database-local cleanup because a real passive cluster never stores the
	// active cluster's transient buffer.
	BailClearBufferedEvents BailReason = "clear-buffered-events"
	// BailNoMutableState means no mutable state was loaded, so there is nothing to
	// convert into an artifact.
	BailNoMutableState BailReason = "no-mutable-state"
)

// Harness is shared by workflow-update hooks in one cluster. It owns artifact
// construction, synchronous passive application, and assertion counters.
//
// The counters matter more than they look. A bail-out is safe, but it is also the
// primary way this harness can lie: a bailed-out write
// commits *active*-generated tasks, which can mask exactly the refresher gap under
// test, while the suite still passes green. So tests assert on these, per reason,
// rather than merely logging them.
type Harness struct {
	logger     log.Logger
	serializer serialization.Serializer

	mu          sync.Mutex
	intercepted int
	diverted    int
	applied     int
	bailouts    map[BailReason]int
	applyErrs   []error

	workflows         map[definition.WorkflowKey]struct{}
	expectedTasks     map[definition.WorkflowKey]map[historytasks.Category][]historytasks.Task
	allowedExtraTasks map[string]struct{}
	standbyExecutions int
	compareTasks      bool
}

// Intercepted is the number of workflow updates observed by the test hook.
func (h *Harness) Intercepted() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.intercepted
}

// ActiveAttempts is the number of classified active writes: diverted writes plus
// active bailouts. It excludes the passive commit's recursive interception.
func (h *Harness) ActiveAttempts() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	count := h.diverted
	for reason, bailouts := range h.bailouts {
		if reason != BailPassivePolicy {
			count += bailouts
		}
	}
	return count
}

func NewHarness(logger log.Logger) *Harness {
	return &Harness{
		logger:            logger,
		serializer:        serialization.NewSerializer(),
		bailouts:          make(map[BailReason]int),
		workflows:         make(map[definition.WorkflowKey]struct{}),
		expectedTasks:     make(map[definition.WorkflowKey]map[historytasks.Category][]historytasks.Task),
		allowedExtraTasks: make(map[string]struct{}),
		compareTasks:      true,
	}
}

func (h *Harness) AllowPassiveOnlyTaskTypes(taskTypes ...string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, taskType := range taskTypes {
		h.allowedExtraTasks[taskType] = struct{}{}
	}
}

func (h *Harness) DisableTaskComparison() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.compareTasks = false
}

func (h *Harness) expectPassiveTasks(
	workflowKey definition.WorkflowKey,
	tasks map[historytasks.Category][]historytasks.Task,
) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.expectedTasks[workflowKey] = tasks
}

// comparePassiveTasks verifies exact task-generation parity at the recursive passive
// commit. Both missing and passive-only tasks are failures, including differences in
// multiplicity.
func (h *Harness) comparePassiveTasks(
	workflowKey definition.WorkflowKey,
	mutableState historyi.MutableState,
	passiveTasks map[historytasks.Category][]historytasks.Task,
) error {
	h.mu.Lock()
	expected, ok := h.expectedTasks[workflowKey]
	if ok {
		delete(h.expectedTasks, workflowKey)
	}
	compareTasks := h.compareTasks
	allowedExtraTasks := make(map[string]struct{}, len(h.allowedExtraTasks))
	for taskType := range h.allowedExtraTasks {
		allowedExtraTasks[taskType] = struct{}{}
	}
	h.mu.Unlock()
	if !ok || !compareTasks {
		return nil
	}

	stickyWorkflowTaskEvents := stickyWorkflowTaskEventIDs(expected)
	expectedFingerprints, err := taskFingerprintsForComparison(expected, taskFingerprintOptions{
		stickyWorkflowTaskEvents: stickyWorkflowTaskEvents,
		ignoreStickyTimeouts:     true,
	})
	if err != nil {
		return fmt.Errorf("passivepath: fingerprint active tasks for %s: %w", workflowKey.String(), err)
	}
	actualFingerprints, err := taskFingerprintsForComparison(passiveTasks, taskFingerprintOptions{
		stickyWorkflowTaskEvents: stickyWorkflowTaskEvents,
	})
	if err != nil {
		return fmt.Errorf("passivepath: fingerprint passive tasks for %s: %w", workflowKey.String(), err)
	}
	missing := missingTaskFingerprints(expectedFingerprints, actualFingerprints)
	extra := missingTaskFingerprints(actualFingerprints, expectedFingerprints)
	extra = slices.DeleteFunc(extra, func(fingerprint string) bool {
		payloadSeparator := strings.IndexByte(fingerprint, '/')
		if payloadSeparator == -1 {
			return false
		}
		payloadSeparator += 1 + strings.IndexByte(fingerprint[payloadSeparator+1:], '/')
		if payloadSeparator == 0 {
			return false
		}
		_, allowed := allowedExtraTasks[fingerprint[:payloadSeparator]]
		return allowed
	})
	if len(missing) != 0 || len(extra) != 0 {
		executionInfo := mutableState.GetExecutionInfo()
		return fmt.Errorf("passivepath: active/passive task generation differs for %s: missing=%v extra=%v active=%v passive=%v "+
			"currentTransition=%v executionStateTransition=%v visibilityTransition=%v workflowTaskTransition=%v",
			workflowKey.String(), missing, extra, expectedFingerprints, actualFingerprints,
			mutableState.CurrentVersionedTransition(),
			mutableState.GetExecutionState().GetLastUpdateVersionedTransition(),
			executionInfo.GetVisibilityLastUpdateVersionedTransition(),
			executionInfo.GetWorkflowTaskLastUpdateVersionedTransition())
	}
	return nil
}

func missingTaskFingerprints(expected, actual []string) []string {
	actualCounts := make(map[string]int, len(actual))
	for _, fingerprint := range actual {
		actualCounts[fingerprint]++
	}
	var missing []string
	for _, fingerprint := range expected {
		if actualCounts[fingerprint] == 0 {
			missing = append(missing, fingerprint)
			continue
		}
		actualCounts[fingerprint]--
	}
	return missing
}

func taskFingerprints(tasksByCategory map[historytasks.Category][]historytasks.Task) ([]string, error) {
	return taskFingerprintsForComparison(tasksByCategory, taskFingerprintOptions{})
}

type taskFingerprintOptions struct {
	stickyWorkflowTaskEvents map[int64]struct{}
	ignoreStickyTimeouts     bool
}

func stickyWorkflowTaskEventIDs(tasksByCategory map[historytasks.Category][]historytasks.Task) map[int64]struct{} {
	eventIDs := make(map[int64]struct{})
	for _, task := range tasksByCategory[historytasks.CategoryTimer] {
		workflowTaskTimeout, ok := task.(*historytasks.WorkflowTaskTimeoutTask)
		if ok && workflowTaskTimeout.TimeoutType == enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START {
			eventIDs[workflowTaskTimeout.EventID] = struct{}{}
		}
	}
	return eventIDs
}

func taskFingerprintsForComparison(
	tasksByCategory map[historytasks.Category][]historytasks.Task,
	options taskFingerprintOptions,
) ([]string, error) {
	var fingerprints []string
	for category, taskList := range tasksByCategory {
		if category == historytasks.CategoryReplication {
			continue
		}
		for _, task := range taskList {
			if workflowTaskTimeout, ok := task.(*historytasks.WorkflowTaskTimeoutTask); ok &&
				options.ignoreStickyTimeouts &&
				workflowTaskTimeout.TimeoutType == enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START {
				continue
			}
			payload, err := json.Marshal(task)
			if err != nil {
				return nil, err
			}
			var normalized any
			if err := json.Unmarshal(payload, &normalized); err != nil {
				return nil, err
			}
			removeTaskIDs(normalized)
			if workflowTask, ok := task.(*historytasks.WorkflowTask); ok {
				if _, sticky := options.stickyWorkflowTaskEvents[workflowTask.ScheduledEventID]; sticky {
					removeField(normalized, "TaskQueue")
				}
			}
			// These task deadlines include independently generated jitter. The rest of
			// their payload must match, while deterministic timer deadlines remain exact.
			if task.GetType() == enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT ||
				task.GetType() == enumsspb.TASK_TYPE_ARCHIVAL_ARCHIVE_EXECUTION {
				removeField(normalized, "VisibilityTimestamp")
			}
			payload, err = json.Marshal(normalized)
			if err != nil {
				return nil, err
			}
			fingerprints = append(fingerprints, fmt.Sprintf("%s/%s/%s", category.Name(), task.GetType(), payload))
		}
	}
	slices.Sort(fingerprints)
	return fingerprints, nil
}

func removeTaskIDs(value any) {
	removeField(value, "TaskID")
}

func removeField(value any, field string) {
	switch value := value.(type) {
	case map[string]any:
		delete(value, field)
		for _, child := range value {
			removeField(child, field)
		}
	case []any:
		for _, child := range value {
			removeField(child, field)
		}
	default:
		return
	}
}

func (h *Harness) recordIntercepted() {
	h.mu.Lock()
	h.intercepted++
	h.mu.Unlock()
}

// newRetriever builds a SyncStateRetriever for artifact construction.
//
// workflowCache, workflowConsistencyChecker and eventBlobCache are nil because the
// mutation-only path neither takes another lease nor reads already-persisted events.
func (h *Harness) newRetriever(shardContext historyi.ShardContext) replication.SyncStateRetriever {
	return replication.NewSyncStateRetriever(shardContext, nil, nil, nil, h.logger)
}

func (h *Harness) recordDiverted(workflowKeys ...definition.WorkflowKey) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.diverted++
	for _, workflowKey := range workflowKeys {
		h.workflows[workflowKey] = struct{}{}
	}
}

// ShouldExecuteTaskAsPassive selects tasks belonging to workflows whose active writes were
// diverted. The router first runs the standby executor: ErrTaskRetry proceeds to the
// active executor, while nil means standby validated the task as safe to drop.
func (h *Harness) ShouldExecuteTaskAsPassive(task historytasks.Task) bool {
	workflowKey := definition.NewWorkflowKey(
		task.GetNamespaceID(),
		task.GetWorkflowID(),
		task.GetRunID(),
	)

	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.workflows[workflowKey]; !ok {
		return false
	}
	h.standbyExecutions++
	return true
}

// StandbyExecutions returns the number of tasks selected for standby-first execution.
func (h *Harness) StandbyExecutions() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.standbyExecutions
}

func (h *Harness) logArtifactEvents(
	workflowKey definition.WorkflowKey,
	count int,
	firstID int64,
	lastID int64,
	nextEventID int64,
	eventTypes []string,
) {
	h.logger.Info(fmt.Sprintf("PPEVT workflow=%s count=%d range=[%d..%d] nextEventID=%d types=%v",
		workflowKey.String(), count, firstID, lastID, nextEventID, eventTypes))
}

func (h *Harness) recordApplied() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.applied++
}

func (h *Harness) recordBailout(reason BailReason) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.bailouts[reason]++
}

// recordApplyError captures a failure from the synchronous passive apply.
func (h *Harness) recordApplyError(err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.applyErrs = append(h.applyErrs, err)
	h.logger.Error(fmt.Sprintf("passivepath: applying replication artifact failed: %v", err))
}

// Diverted is the number of active writes routed through the passive apply path.
func (h *Harness) Diverted() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.diverted
}

// Applied is the number of artifacts successfully applied.
func (h *Harness) Applied() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.applied
}

// Bailouts returns the per-reason bail-out counts, excluding BailPassivePolicy, which
// is normal and expected (it is how the replicator's own write gets committed).
func (h *Harness) Bailouts() map[BailReason]int {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make(map[BailReason]int, len(h.bailouts))
	for reason, count := range h.bailouts {
		if reason == BailPassivePolicy {
			continue
		}
		out[reason] = count
	}
	return out
}

// AllBailouts returns every bail-out count including BailPassivePolicy. Diagnostic only;
// assertions should use Bailouts.
func (h *Harness) AllBailouts() map[BailReason]int {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make(map[BailReason]int, len(h.bailouts))
	maps.Copy(out, h.bailouts)
	return out
}

// ApplyErrors returns failures encountered while applying artifacts.
func (h *Harness) ApplyErrors() []error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]error(nil), h.applyErrs...)
}
