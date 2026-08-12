package model

import (
	"context"
	"iter"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/fact"
)

const ActivityType = fact.ActivityType

var _ umpire.Entity = (*Activity)(nil)
var _ umpire.Lifecycled = (*Activity)(nil)

// Activity is the observed standalone activity lifecycle and its public links.
type Activity struct {
	NamespaceID string
	ActivityID  string
	Links       []*commonpb.Link
	FSM         *umpire.Lifecycle
}

func NewActivity() *Activity {
	return &Activity{FSM: NewActivityLifecycle()}
}

func (*Activity) Type() umpire.EntityType        { return ActivityType }
func (a *Activity) Lifecycle() *umpire.Lifecycle { return a.FSM }

func (a *Activity) OnFact(ctx context.Context, _ *umpire.EntityPath, facts iter.Seq[umpire.Fact]) error {
	for raw := range facts {
		snapshot, ok := raw.(*fact.ActivityExecutionSnapshot)
		if !ok {
			continue
		}
		a.ActivityID = snapshot.ActivityID
		a.NamespaceID = snapshot.NamespaceID
		a.Links = snapshot.Links
		switch snapshot.Status {
		case enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING:
			a.advanceToStarted(ctx)
		case enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED:
			a.advanceToStarted(ctx)
			if a.FSM.Current() == ActivityStarted {
				a.FSM.Fire(ctx, ActivityComplete)
			}
		case enumspb.ACTIVITY_EXECUTION_STATUS_FAILED:
			a.advanceToScheduled(ctx)
			if a.FSM.Current() == ActivityScheduled || a.FSM.Current() == ActivityStarted {
				a.FSM.Fire(ctx, ActivityFail)
			}
		case enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT:
			a.advanceToScheduled(ctx)
			if !a.FSM.IsTerminal() {
				a.FSM.Fire(ctx, ActivityTimeout)
			}
		case enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED:
			a.advanceToScheduled(ctx)
			if !a.FSM.IsTerminal() {
				a.FSM.Fire(ctx, ActivityCancel)
			}
		default:
			continue
		}
	}
	return nil
}

func (a *Activity) advanceToScheduled(ctx context.Context) {
	if a.FSM.Current() == ActivityUnspecified {
		a.FSM.Fire(ctx, ActivitySchedule)
	}
}

func (a *Activity) advanceToStarted(ctx context.Context) {
	a.advanceToScheduled(ctx)
	if a.FSM.Current() == ActivityScheduled {
		a.FSM.Fire(ctx, ActivityStart)
	}
}
