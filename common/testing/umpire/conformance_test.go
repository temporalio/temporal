package umpire

import (
	"context"
	"iter"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
)

// condFact carries an FSM event name for condEntity to fire.
type condFact struct {
	target *EntityPath
	event  string
}

func (f *condFact) Name() string              { return "condFact" }
func (f *condFact) TargetEntity() *EntityPath { return f.target }

// condEntity is a Lifecycled test entity over branchSpec (isolated branches), so an
// out-of-branch event is a genuinely illegal transition that Fire records.
type condEntity struct{ fsm *Lifecycle }

func (e *condEntity) Type() EntityType      { return "condEntity" }
func (e *condEntity) Lifecycle() *Lifecycle { return e.fsm }
func (e *condEntity) OnFact(ctx context.Context, _ *EntityPath, facts iter.Seq[Fact]) error {
	for f := range facts {
		if cf, ok := f.(*condFact); ok {
			e.fsm.Fire(ctx, cf.event)
		}
	}
	return nil
}

// The built-in conformance check surfaces the illegal transitions the model records at
// fire-time (from OnFact → Fire → Classify == Illegal) as violations — with no rule
// registered — and reports each one once.
func TestRuleRegistry_ConformanceSurfacesIllegalTransitions(t *testing.T) {
	ctx := context.Background()
	ms := NewModelState()
	ms.RegisterEntity(func() Entity { return &condEntity{fsm: NewLifecycle(branchSpec())} }, &condFact{})

	rr := NewRuleRegistry()
	require.NoError(t, rr.InitRules(ms, log.NewNoopLogger(), RuleConfig{}))

	target := &EntityPath{EntityID: NewEntityID("condEntity", "x")}
	// A legal edge, then an out-of-branch (illegal) edge.
	require.NoError(t, ms.RouteFacts(ctx, []Fact{&condFact{target: target, event: "toB"}}))
	require.NoError(t, ms.RouteFacts(ctx, []Fact{&condFact{target: target, event: "toC"}}))

	v := rr.Check(ctx, true, nil)
	require.Len(t, v, 1)
	require.Equal(t, "Conformance", v[0].Rule)
	require.Equal(t, "toC", v[0].Tags["event"])
	require.Equal(t, "b", v[0].Tags["from"])

	// Deduped: a second check does not re-report the same illegal transition.
	require.Empty(t, rr.Check(ctx, true, nil))

	// Purge clears the dedup state so a fresh run would report it again.
	rr.PurgeScope(NewEntityID("condEntity", "x"))
	require.Len(t, rr.Check(ctx, true, nil), 1)
}

func TestRuleRegistry_RetainsScopedRecordedConformanceViolations(t *testing.T) {
	ms := NewModelState()
	rr := NewRuleRegistry()
	require.NoError(t, rr.InitRules(ms, log.NewNoopLogger(), RuleConfig{}))
	left := NewEntityID("Namespace", "left")
	right := NewEntityID("Namespace", "right")
	violation := Violation{
		Rule:    "Conformance",
		Message: "callback relation conflict",
		Tags:    map[string]string{"relation": "callback-operation"},
	}

	rr.RecordConformance(left, "callback-operation:callback", violation)
	rr.RecordConformance(left, "callback-operation:callback", violation)
	rr.RecordConformance(right, "callback-operation:other", violation)
	require.Len(t, rr.Check(context.Background(), false, &left), 1)
	require.Len(t, rr.Check(context.Background(), false, &left), 1, "recorded conformance remains visible until purge")
	require.Len(t, rr.Check(context.Background(), false, nil), 2)

	rr.PurgeScope(left)
	require.Empty(t, rr.Check(context.Background(), false, &left))
	require.Len(t, rr.Check(context.Background(), false, &right), 1)
}
