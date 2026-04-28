package umpire

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestComponentsValidate_Empty(t *testing.T) {
	require.NoError(t, Components{}.Validate())
}

func TestComponentsValidate_Valid(t *testing.T) {
	cs := Components{
		{
			Name: "a",
			RPCs: []RPCSpec{{Method: "/svc/A"}},
			Faults: map[string]MethodFault{
				"/svc/A": {},
			},
			ObservedMethods: []string{"/svc/A"},
		},
		{
			Name: "b",
			RPCs: []RPCSpec{{Method: "/svc/B"}},
		},
	}
	require.NoError(t, cs.Validate())
}

func TestComponentsValidate_EmptyName(t *testing.T) {
	cs := Components{{}}
	err := cs.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty Name")
}

func TestComponentsValidate_DuplicateName(t *testing.T) {
	cs := Components{{Name: "a"}, {Name: "a"}}
	err := cs.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), `name "a" declared 2 times`)
}

func TestComponentsValidate_DuplicateRPC(t *testing.T) {
	cs := Components{
		{Name: "a", RPCs: []RPCSpec{{Method: "/svc/X"}}},
		{Name: "b", RPCs: []RPCSpec{{Method: "/svc/X"}}},
	}
	err := cs.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), `RPC "/svc/X" claimed by components "a" and "b"`)
}

func TestComponentsValidate_OrphanFault(t *testing.T) {
	cs := Components{
		{
			Name:   "a",
			Faults: map[string]MethodFault{"/svc/Missing": {}},
		},
	}
	err := cs.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), `fault for "/svc/Missing" with no matching RPC`)
}

func TestComponentsValidate_OrphanObservedMethod(t *testing.T) {
	cs := Components{
		{
			Name:            "a",
			ObservedMethods: []string{"/svc/Ghost"},
		},
	}
	err := cs.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), `observes "/svc/Ghost" with no matching RPC`)
}

func TestComponentsValidate_DuplicateFault(t *testing.T) {
	cs := Components{
		{
			Name:   "a",
			RPCs:   []RPCSpec{{Method: "/svc/A"}},
			Faults: map[string]MethodFault{"/svc/A": {}},
		},
		{
			Name:   "b",
			Faults: map[string]MethodFault{"/svc/A": {}},
		},
	}
	err := cs.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), `fault for "/svc/A" declared by both "a" and "b"`)
}

func TestComponentsBehaviors_SkipsNil(t *testing.T) {
	cs := Components{
		{Name: "a"},                                     // no Behavior
		{Name: "b", Behavior: stubModelBehavior{}},      // present
	}
	require.Len(t, cs.Behaviors(), 1)
}

func TestComponentsStrategies_SkipsZero(t *testing.T) {
	cs := Components{
		{Name: "a"},                                                  // zero Strategy
		{Name: "b", Observer: Strategy{Name: "x"}},                   // no Client/Server fns
		{Name: "c", Observer: Strategy{Server: stubServerInterceptor}}, // real
	}
	require.Len(t, cs.Strategies(), 1)
}

func TestComponentsRules_Merges(t *testing.T) {
	a := &RuleSet{}
	a.Add(SafetyRule{Name: "rule-a", Check: func(*RuleContext, []*Record) {}})
	b := &RuleSet{}
	b.Add(SafetyRule{Name: "rule-b", Check: func(*RuleContext, []*Record) {}})

	cs := Components{{Name: "a", Rules: a}, {Name: "b", Rules: b}}
	merged := cs.Rules()
	require.Len(t, merged.Rules(), 2)
}

func TestComponentsRPCs_Sorted(t *testing.T) {
	cs := Components{
		{Name: "a", RPCs: []RPCSpec{{Method: "/svc/B"}}},
		{Name: "b", RPCs: []RPCSpec{{Method: "/svc/A"}}},
	}
	specs := cs.RPCs()
	require.Len(t, specs, 2)
	require.Equal(t, "/svc/A", specs[0].Method)
	require.Equal(t, "/svc/B", specs[1].Method)
}

func TestComponentsFaults_Merges(t *testing.T) {
	cs := Components{
		{
			Name:   "a",
			RPCs:   []RPCSpec{{Method: "/svc/A"}},
			Faults: map[string]MethodFault{"/svc/A": {Delay: Pct(50)}},
		},
		{
			Name:   "b",
			RPCs:   []RPCSpec{{Method: "/svc/B"}},
			Faults: map[string]MethodFault{"/svc/B": {Delay: Pct(75)}},
		},
	}
	merged := cs.Faults()
	require.Len(t, merged, 2)
	require.Equal(t, Pct(50), merged["/svc/A"].Delay)
	require.Equal(t, Pct(75), merged["/svc/B"].Delay)
}

type stubModelBehavior struct{}

func (stubModelBehavior) Actions() []Action       { return nil }
func (stubModelBehavior) CheckInvariant(t *T)     {}
func (stubModelBehavior) Cleanup(t *T)            {}

var stubServerInterceptor grpc.UnaryServerInterceptor = func(
	_ context.Context,
	_ any,
	_ *grpc.UnaryServerInfo,
	_ grpc.UnaryHandler,
) (any, error) {
	return nil, nil
}
