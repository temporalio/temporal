package scheduler

import (
	"fmt"
	"time"

	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/worker/scheduler"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	// Scheduler is a top-level state machine compromised of 3 sub state machines:
	// - Generator: buffers actions according to the schedule specification
	// - Invoker: executes buffered actions
	// - Backfiller: buffers actions according to requested backfills
	//
	// A running Scheduler will always have exactly one of each of the above sub state
	// machines mounted as nodes within the HSM tree. The top-level	machine itself
	// remains in a singular running state for its lifetime (all work is done within the
	// sub state machines). The Scheduler state machine is only responsible for creating
	// the singleton sub state machines.
	Scheduler struct {
		*schedulespb.SchedulerInternal

		// Locally-cached state, invalidated whenever cacheConflictToken != ConflictToken.
		cacheConflictToken int64
		compiledSpec       *scheduler.CompiledSpec
	}

	// The machine definitions provide serialization/deserialization and type information.
	schedulerMachineDefinition struct{}

	SchedulerMachineState int
)

const (
	// Unique identifier for top-level scheduler state machine.
	SchedulerMachineType = "scheduler.Scheduler"

	// The top-level scheduler only has a single, constant state.
	SchedulerMachineStateRunning SchedulerMachineState = 0

	// How many recent actions to keep on the Info.RecentActions list.
	recentActionCount = 10
)

var (
	_ hsm.StateMachine[SchedulerMachineState] = Scheduler{}
	_ hsm.StateMachineDefinition              = &schedulerMachineDefinition{}
)

// NewScheduler returns an initialized Scheduler state machine (without any sub
// state machines).
func NewScheduler(
	namespace, namespaceID, scheduleID string,
	sched *schedulepb.Schedule,
	patch *schedulepb.SchedulePatch,
) *Scheduler {
	var zero time.Time
	return &Scheduler{
		SchedulerInternal: &schedulespb.SchedulerInternal{
			Schedule: sched,
			Info: &schedulepb.ScheduleInfo{
				ActionCount:         0,
				MissedCatchupWindow: 0,
				OverlapSkipped:      0,
				BufferDropped:       0,
				BufferSize:          0,
				RunningWorkflows:    []*commonpb.WorkflowExecution{},
				RecentActions:       []*schedulepb.ScheduleActionResult{},
				FutureActionTimes:   []*timestamppb.Timestamp{},
				CreateTime:          timestamppb.Now(),
				UpdateTime:          timestamppb.New(zero),
			},
			InitialPatch:  patch,
			Namespace:     namespace,
			NamespaceId:   namespaceID,
			ScheduleId:    scheduleID,
			ConflictToken: scheduler.InitialConflictToken,
		},
		cacheConflictToken: scheduler.InitialConflictToken,
		compiledSpec:       nil,
	}
}

// RegisterStateMachine registers state machine definitions with the HSM
// registry. Should be called during dependency injection.
func RegisterStateMachines(r *hsm.Registry) error {
	if err := r.RegisterMachine(schedulerMachineDefinition{}); err != nil {
		return err
	}
	if err := r.RegisterMachine(generatorMachineDefinition{}); err != nil {
		return err
	}
	if err := r.RegisterMachine(invokerMachineDefinition{}); err != nil {
		return err
	}
	return r.RegisterMachine(backfillerMachineDefinition{})
}

func (s Scheduler) State() SchedulerMachineState {
	return SchedulerMachineStateRunning
}

func (s Scheduler) SetState(_ SchedulerMachineState) {}

func (s Scheduler) RegenerateTasks(node *hsm.Node) ([]hsm.Task, error) {
	// The top level scheduler has no tasks of its own.
	return nil, nil
}

func (schedulerMachineDefinition) Type() string {
	return SchedulerMachineType
}

func (schedulerMachineDefinition) Serialize(state any) ([]byte, error) {
	if state, ok := state.(Scheduler); ok {
		return proto.Marshal(state.SchedulerInternal)
	}
	return nil, fmt.Errorf("invalid scheduler state provided: %v", state)
}

func (schedulerMachineDefinition) Deserialize(body []byte) (any, error) {
	state := &schedulespb.SchedulerInternal{}
	return Scheduler{
		SchedulerInternal: state,
		compiledSpec:      nil,
	}, proto.Unmarshal(body, state)
}

func (schedulerMachineDefinition) CompareState(a any, b any) (int, error) {
	panic("TODO: CompareState not yet implemented for Scheduler")
}

// useScheduledAction returns true when the Scheduler should allow scheduled
// actions to be taken.
//
// When decrement is true, the schedule's state's `RemainingActions` counter is
// decremented when an action can be taken. When decrement is false, no state
// is mutated.
func (s *Scheduler) useScheduledAction(decrement bool) bool {
	// If paused, don't do anything.
	if s.Schedule.State.Paused {
		return false
	}

	// If unlimited actions, allow.
	if !s.Schedule.State.LimitedActions {
		return true
	}

	// Otherwise check and decrement limit.
	if s.Schedule.State.RemainingActions > 0 {
		if decrement {
			s.Schedule.State.RemainingActions--

			// The conflict token is updated because a client might be in the process of
			// preparing an update request that increments their schedule's RemainingActions
			// field.
			s.updateConflictToken()
		}
		return true
	}

	// No actions left
	return false
}

func (s *Scheduler) getCompiledSpec(specBuilder *scheduler.SpecBuilder) (*scheduler.CompiledSpec, error) {
	s.validateCachedState()

	// Cache compiled spec.
	if s.compiledSpec == nil {
		cspec, err := specBuilder.NewCompiledSpec(s.Schedule.Spec)
		if err != nil {
			return nil, err
		}
		s.compiledSpec = cspec
	}

	return s.compiledSpec, nil
}

func (s Scheduler) jitterSeed() string {
	return fmt.Sprintf("%s-%s", s.NamespaceId, s.ScheduleId)
}

func (s Scheduler) identity() string {
	return fmt.Sprintf("temporal-scheduler-%s-%s", s.Namespace, s.ScheduleId)
}

func (s Scheduler) overlapPolicy() enumspb.ScheduleOverlapPolicy {
	policy := s.Schedule.Policies.OverlapPolicy
	if policy == enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED {
		policy = enumspb.SCHEDULE_OVERLAP_POLICY_SKIP
	}
	return policy
}

func (s Scheduler) resolveOverlapPolicy(overlapPolicy enumspb.ScheduleOverlapPolicy) enumspb.ScheduleOverlapPolicy {
	if overlapPolicy == enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED {
		overlapPolicy = s.overlapPolicy()
	}
	return overlapPolicy
}

// validateCachedState clears cached fields whenever the Scheduler's
// ConflictToken doesn't match its cacheConflictToken field. Validation is only
// as effective as the Scheduler's backing persisted state is up-to-date.
func (s *Scheduler) validateCachedState() {
	if s.cacheConflictToken != s.ConflictToken {
		// Bust stale cached fields.
		s.compiledSpec = nil

		// We're now up-to-date.
		s.cacheConflictToken = s.ConflictToken
	}
}

// updateConflictToken bumps the Scheduler's conflict token. This has a side
// effect of invalidating the local cache. Use whenever applying a mutation that
// should invalidate other in-flight updates.
func (s *Scheduler) updateConflictToken() {
	s.ConflictToken++
}

// EnqueueBufferedStarts enqueues the given starts onto a scheduler tree's
// Invoker for execution.
func (s *Scheduler) EnqueueBufferedStarts(
	node *hsm.Node,
	starts []*schedulespb.BufferedStart,
) error {
	invokerNode, err := node.Child([]hsm.Key{InvokerMachineKey})
	if err != nil {
		return err
	}
	err = hsm.MachineTransition(invokerNode, func(e Invoker) (hsm.TransitionOutput, error) {
		return TransitionEnqueue.Apply(e, EventEnqueue{
			BufferedStarts: starts,
		})
	})
	return err
}

// RequestBackfill spawns a new Backfiller node to the scheduler tree for a
// BackfillRequest.
func (s Scheduler) RequestBackfill(
	env hsm.Environment,
	node *hsm.Node,
	request *schedulepb.BackfillRequest,
) (hsm.TransitionOutput, error) {
	id := uuid.New()
	backfiller := Backfiller{
		BackfillerInternal: &schedulespb.BackfillerInternal{
			Request:           &schedulespb.BackfillerInternal_BackfillRequest{BackfillRequest: request},
			BackfillId:        id,
			LastProcessedTime: timestamppb.New(env.Now()),
		},
	}

	_, err := node.AddChild(BackfillerMachineKey(id), backfiller)
	if err != nil {
		return hsm.TransitionOutput{}, err
	}

	return backfiller.output()
}

// RequestImmediate spawns a new Backfiller node to the scheduler tree for a
// TriggerImmediately request.
func (s Scheduler) RequestImmediate(
	env hsm.Environment,
	node *hsm.Node,
	trigger *schedulepb.TriggerImmediatelyRequest,
) (hsm.TransitionOutput, error) {
	id := uuid.New()
	backfiller := Backfiller{
		BackfillerInternal: &schedulespb.BackfillerInternal{
			Request:           &schedulespb.BackfillerInternal_TriggerRequest{TriggerRequest: trigger},
			BackfillId:        id,
			LastProcessedTime: timestamppb.New(env.Now()),
		},
	}

	_, err := node.AddChild(BackfillerMachineKey(id), backfiller)
	if err != nil {
		return hsm.TransitionOutput{}, err
	}

	return backfiller.output()
}

type EventRecordAction struct {
	Node *hsm.Node

	ActionCount         int64
	OverlapSkipped      int64
	BufferDropped       int64
	MissedCatchupWindow int64
	Results             []*schedulepb.ScheduleActionResult
}

// Fired when an action has been taken by the state machine scheduler and should
// be recorded.
var TransitionRecordAction = hsm.NewTransition(
	[]SchedulerMachineState{SchedulerMachineStateRunning},
	SchedulerMachineStateRunning,
	func(s Scheduler, event EventRecordAction) (hsm.TransitionOutput, error) {
		s.Info.ActionCount += event.ActionCount
		s.Info.OverlapSkipped += event.OverlapSkipped
		s.Info.BufferDropped += event.BufferDropped
		s.Info.MissedCatchupWindow += event.MissedCatchupWindow

		if len(event.Results) > 0 {
			s.Info.RecentActions = util.SliceTail(append(s.Info.RecentActions, event.Results...), recentActionCount)
		}

		for _, result := range event.Results {
			if result.StartWorkflowResult != nil {
				s.Info.RunningWorkflows = append(s.Info.RunningWorkflows, result.StartWorkflowResult)
			}
		}

		return hsm.TransitionOutput{}, nil
	},
)
