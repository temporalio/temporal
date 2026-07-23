package model

import (
	"errors"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
)

var (
	ErrAlreadyCreated = errors.New("schedule already created")
	ErrNotCreated     = errors.New("schedule not created")
	ErrClosed         = errors.New("schedule closed")
	ErrInvalidEvent   = errors.New("invalid scheduler event")
)

type Config struct {
	StartTime         time.Time
	Interval          time.Duration
	Phase             time.Duration
	SpecStart         time.Time
	SpecEnd           time.Time
	CatchupWindow     time.Duration
	RecentActionLimit int
	MaxGenerated      int
	OverlapPolicy     enumspb.ScheduleOverlapPolicy
	LimitedActions    bool
	RemainingActions  int64
}

type Lifecycle int

const (
	LifecycleAbsent Lifecycle = iota
	LifecycleRunning
	LifecycleClosed
)

type Action struct {
	ID          string
	NominalTime time.Time
	Manual      bool
	RunID       string
	Dispatched  bool
	Attempt     int
	RetryAt     time.Time
}

type State struct {
	Now              time.Time
	Lifecycle        Lifecycle
	Paused           bool
	Notes            string
	ConflictToken    int64
	HighWatermark    time.Time
	Pending          []Action
	Running          []Action
	Recent           []Action
	ActionCount      int64
	OverlapSkipped   int64
	LimitedActions   bool
	RemainingActions int64
	OverlapPolicy    enumspb.ScheduleOverlapPolicy
}

type Event interface {
	isEvent()
}

type Create struct {
	Paused bool
	Notes  string
}

type Describe struct{}

type Pause struct {
	Note string
}

type Unpause struct {
	Note string
}

type Update struct {
	Paused        bool
	Notes         string
	OverlapPolicy enumspb.ScheduleOverlapPolicy
}

type Trigger struct {
	ID string
}

type Backfill struct {
	ID            string
	Start         time.Time
	End           time.Time
	OverlapPolicy enumspb.ScheduleOverlapPolicy
}

type AdvanceTime struct {
	Time time.Time
}

type StartSucceeded struct {
	ActionID string
	RunID    string
}

type StartFailureClass int

const (
	StartFailureRetryable StartFailureClass = iota
	StartFailureRateLimited
	StartFailureNonRetryable
)

type StartFailed struct {
	ActionID string
	Class    StartFailureClass
	RetryAt  time.Time
}

type CompleteWorkflow struct {
	ActionID string
}

type Delete struct{}

func (Create) isEvent()           {}
func (Describe) isEvent()         {}
func (Pause) isEvent()            {}
func (Unpause) isEvent()          {}
func (Update) isEvent()           {}
func (Trigger) isEvent()          {}
func (Backfill) isEvent()         {}
func (AdvanceTime) isEvent()      {}
func (StartSucceeded) isEvent()   {}
func (StartFailed) isEvent()      {}
func (CompleteWorkflow) isEvent() {}
func (Delete) isEvent()           {}

type Effect interface {
	isEffect()
}

type StartWorkflow struct {
	Action Action
}

type ScheduleWakeup struct {
	At time.Time
}

type CloseSchedule struct{}

type CancelWorkflow struct {
	ActionID string
}

type TerminateWorkflow struct {
	ActionID string
}

func (StartWorkflow) isEffect()     {}
func (ScheduleWakeup) isEffect()    {}
func (CloseSchedule) isEffect()     {}
func (CancelWorkflow) isEffect()    {}
func (TerminateWorkflow) isEffect() {}

type Response interface {
	isResponse()
}

type MutationResponse struct {
	ConflictToken int64
}

type DescribeResponse struct {
	Observable Observable
}

func (MutationResponse) isResponse() {}
func (DescribeResponse) isResponse() {}

type Outcome struct {
	State    State
	Effects  []Effect
	Response Response
}

type Observable struct {
	Now              time.Time
	Lifecycle        Lifecycle
	Paused           bool
	Notes            string
	ConflictToken    int64
	Pending          int
	Running          int
	Recent           int
	ActionCount      int64
	OverlapSkipped   int64
	LimitedActions   bool
	RemainingActions int64
}

func Observe(state State) Observable {
	return Observable{
		Now:              state.Now,
		Lifecycle:        state.Lifecycle,
		Paused:           state.Paused,
		Notes:            state.Notes,
		ConflictToken:    state.ConflictToken,
		Pending:          len(state.Pending),
		Running:          len(state.Running),
		Recent:           len(state.Recent),
		ActionCount:      state.ActionCount,
		OverlapSkipped:   state.OverlapSkipped,
		LimitedActions:   state.LimitedActions,
		RemainingActions: state.RemainingActions,
	}
}
