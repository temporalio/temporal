package umpire

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"pgregory.net/rapid"
)

type testFact struct {
	key string
}

func (f testFact) Key() string {
	return f.key
}

var finalLivenessRule = LivenessRule{
	Name: "final-liveness",
	Check: func(ctx *RuleContext, history []*Record, final bool) {
		if !final {
			return
		}
		ctx.Violate("pending condition", nil)
	},
}

type modelWithPrecondition struct {
	Model[*modelWithPrecondition]
	enabled bool
}

var _ ModelBehavior = (*modelWithPrecondition)(nil)

func (m *modelWithPrecondition) Actions() []Action {
	return []Action{{
		Name:    "Action",
		Enabled: func() bool { return m.enabled },
		Run:     func(t *T) {},
	}}
}

func (m *modelWithPrecondition) CheckInvariant(t *T) {}

func (m *modelWithPrecondition) Cleanup(t *T) {}

type badExplicitActionModel struct {
	Model[*badExplicitActionModel]
}

func (m *badExplicitActionModel) Actions() []Action {
	return []Action{{Name: "Broken"}}
}

func (m *badExplicitActionModel) CheckInvariant(t *T) {}

func (m *badExplicitActionModel) Cleanup(t *T) {}

type modelWithRules struct {
	Model[*modelWithRules]
}

func (m *modelWithRules) Actions() []Action {
	return nil
}

func (m *modelWithRules) CheckInvariant(t *T) {}

func (m *modelWithRules) Cleanup(t *T) {}

func (m *modelWithRules) Rules() *RuleSet {
	rs := &RuleSet{}
	rs.Add(finalLivenessRule)
	return rs
}

type modelWithAssertions struct {
	Model[*modelWithAssertions]
}

type compositeTestComponent struct {
	name  string
	order *[]string
	rules *RuleSet
}

func (c compositeTestComponent) Actions() []Action {
	return []Action{{Name: c.name, Run: func(t *T) {}}}
}

func (c compositeTestComponent) CheckInvariant(t *T) {
	*c.order = append(*c.order, "check-"+c.name)
}

func (c compositeTestComponent) Cleanup(t *T) {
	*c.order = append(*c.order, "cleanup-"+c.name)
}

func (c compositeTestComponent) Rules() *RuleSet {
	return c.rules
}

func noTestError() error {
	return nil
}

func (m *modelWithAssertions) Actions() []Action {
	return []Action{{
		Name: "Assert",
		Run: func(t *T) {
			require.NotNil(t, t.Assertions)
			require.NotNil(t, m.Assertions)
			t.NoError(noTestError())
			m.NoError(noTestError())
		},
	}}
}

func (m *modelWithAssertions) CheckInvariant(t *T) {
	require.NotNil(t, m.Assertions)
	m.NoError(noTestError())
}

func (m *modelWithAssertions) Cleanup(t *T) {
	require.NotNil(t, m.Assertions)
	m.NoError(noTestError())
}

func TestRecordTransition(t *testing.T) {
	u := &Umpire{}
	status := "running"

	require.False(t, RecordTransition(u, "op1", &status, "running"))
	require.Empty(t, u.History())

	require.True(t, RecordTransition(u, "op1", &status, "completed"))
	require.Equal(t, "completed", status)

	history := u.History()
	require.Len(t, history, 1)
	transition, ok := history[0].Fact.(*Transition[string])
	require.True(t, ok)
	require.Equal(t, "op1", transition.EntityID)
	require.Equal(t, "running", transition.From)
	require.Equal(t, "completed", transition.To)
}

func TestCheckRulesFinalLiveness(t *testing.T) {
	u := &Umpire{}
	u.Record(testFact{key: "op1"})
	u.AddRule(finalLivenessRule)

	require.Empty(t, u.CheckRules(false))

	violations := u.CheckRules(true)
	require.Len(t, violations, 1)
	require.Equal(t, "final-liveness", violations[0].Rule)
	require.Equal(t, "pending condition", violations[0].Message)
}

func TestCheckRulesTracksCoverage(t *testing.T) {
	u := &Umpire{}
	u.startCoverageRun(newCoverageCollector())
	point := CoveragePoint{
		Name:        "state-verified",
		Description: "state was reached and verified",
		MinVerified: 1,
	}
	u.AddRule(SafetyRule{
		Name:     "coverage-rule",
		Coverage: []CoveragePoint{point},
		Check: func(ctx *RuleContext, history []*Record) {
			ctx.Check(point, "op1", true, "unexpected failure", nil)
			ctx.Check(point, "op1", true, "unexpected failure", nil)
		},
	})

	require.Empty(t, u.CheckRules(false))

	summary := u.CoverageSummary()
	require.Len(t, summary, 1)
	require.Equal(t, "coverage-rule", summary[0].Rule)
	require.Equal(t, "state-verified", summary[0].Point)
	require.Equal(t, 1, summary[0].Reached)
	require.Equal(t, 1, summary[0].Verified)
	require.Equal(t, 1, summary[0].MinVerified)
}

func TestCheckRulesCollectsRuleContextViolations(t *testing.T) {
	u := &Umpire{}
	point := CoveragePoint{Name: "bad-state"}
	u.AddRule(SafetyRule{
		Name: "violation-rule",
		Check: func(ctx *RuleContext, history []*Record) {
			ctx.ViolatePoint(point, "bad thing happened", map[string]string{
				"entity": "op1",
			})
		},
	})

	violations := u.CheckRules(false)
	require.Len(t, violations, 1)
	require.Equal(t, "violation-rule", violations[0].Rule)
	require.Equal(t, "bad-state", violations[0].Point)
	require.Equal(t, "bad thing happened", violations[0].Message)
	require.Equal(t, map[string]string{"entity": "op1"}, violations[0].Tags)
}

func TestCoverageSummaryIncludesDeclaredButUnreachedPoints(t *testing.T) {
	coverage := newCoverageCollector()
	coverage.declare("coverage-rule", []CoveragePoint{{
		Name:        "missing-state",
		Description: "state was not reached",
		MinVerified: 2,
	}})

	summary := coverage.summary()
	require.Len(t, summary, 1)
	require.Equal(t, 0, summary[0].Reached)
	require.Equal(t, 0, summary[0].Verified)

	formatted := formatCoverageSummary(summary)
	require.Contains(t, formatted, "coverage-rule")
	require.Contains(t, formatted, "missing-state")
	require.Contains(t, formatted, "missing")
	require.True(t, strings.HasPrefix(formatted, "umpire coverage:"))
}

func TestRegisterRulesFromProvider(t *testing.T) {
	u := &Umpire{}
	m := &modelWithRules{}
	m.Model = NewModel(u, m)

	m.registerRules()

	violations := u.CheckRules(true)
	require.Len(t, violations, 1)
	require.Equal(t, "final-liveness", violations[0].Rule)
}

func TestCompositeAggregatesComponents(t *testing.T) {
	var order []string
	rules1 := (&RuleSet{}).Add(finalLivenessRule)
	rules2 := (&RuleSet{}).Add(finalLivenessRule)
	composite := NewComposite(
		compositeTestComponent{name: "one", order: &order, rules: rules1},
		compositeTestComponent{name: "two", order: &order, rules: rules2},
	)

	var actionNames []string
	for _, action := range composite.Actions() {
		actionNames = append(actionNames, action.Name)
	}
	require.ElementsMatch(t, []string{"one", "two"}, actionNames)

	composite.CheckInvariant(nil)
	composite.Cleanup(nil)
	require.Equal(t, []string{"check-one", "check-two", "cleanup-two", "cleanup-one"}, order)

	merged := composite.Rules().Rules()
	require.Len(t, merged, 1)
	require.Equal(t, "final-liveness", merged[0].(LivenessRule).Name)
}

func TestRunInitializesRequireAssertions(t *testing.T) {
	u := &Umpire{}
	m := &modelWithAssertions{}
	m.Model = NewModel(u, m)

	rapid.Check(t, func(rt *rapid.T) {
		m.Run(newT(rt))
	})
}

func TestMutateProtoRequestClonesRequest(t *testing.T) {
	mutations := NewRequestMutations(WithRequestMutationProbability(1, 1))

	rapid.Check(t, func(rt *rapid.T) {
		pt := newT(rt)
		req := durationpb.New(time.Second)

		mutated, ok := MutateProtoRequest(pt, mutations, "duration", req)

		require.True(t, ok)
		require.NotSame(t, req, mutated)
		require.Equal(t, int64(1), req.Seconds)
		require.Equal(t, int32(0), req.Nanos)
	})
}

func TestRequestMutationInterceptorFallsBackOnClassifiedMutationError(t *testing.T) {
	mutations := NewRequestMutations(WithRequestMutationProbability(1, 1))
	mutatedErr := errors.New("mutated request")

	rapid.Check(t, func(rt *rapid.T) {
		pt := newT(rt)
		mutations.SetRequestMutationT(pt)
		req := durationpb.New(time.Second)
		originalReq := proto.Clone(req)
		reply := &durationpb.Duration{}
		var calls int

		err := mutations.UnaryClientInterceptor(
			nil,
			func(err error) bool {
				return errors.Is(err, mutatedErr)
			},
		)(
			context.Background(),
			"/test.Duration/Call",
			req,
			reply,
			nil,
			func(
				ctx context.Context,
				method string,
				callReq, reply any,
				cc *grpc.ClientConn,
				opts ...grpc.CallOption,
			) error {
				calls++
				if calls == 1 {
					return mutatedErr
				}
				require.True(t, proto.Equal(originalReq, callReq.(*durationpb.Duration)))
				return nil
			},
		)

		require.NoError(t, err)
		require.Equal(t, 2, calls)
		require.True(t, proto.Equal(originalReq, req))
	})
}

func TestActionsFromModelBehavior(t *testing.T) {
	u := &Umpire{}
	m := &modelWithPrecondition{}
	m.Model = NewModel(u, m)

	actions, err := m.actions()
	require.NoError(t, err)
	require.Contains(t, actions, "")
	require.Contains(t, actions, "Action")

	require.False(t, actions["Action"].Enabled())
	m.enabled = true
	require.True(t, actions["Action"].Enabled())
}

func TestActionsRejectsMalformedExplicitAction(t *testing.T) {
	u := &Umpire{}
	m := &badExplicitActionModel{}
	m.Model = NewModel(u, m)

	_, err := m.actions()
	require.ErrorContains(t, err, "Broken")
	require.ErrorContains(t, err, "Run")
}
