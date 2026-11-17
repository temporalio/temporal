package scheduler

import (
	"compress/gzip"
	"encoding/json"
	"io"
	"os"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/testsuite"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// TestTweakablesUnchangedBelowChunkedCatchup checks that, below ChunkedCatchup, this binary
// computes exactly the tweakables that main recorded. The tweakables MutableSideEffect records
// a new marker whenever the computed value differs from the recorded one, so a difference
// (such as MaxCatchupMatchesPerTask=5000 instead of the absent field) would add a marker to
// every running schedule on deploy.
//
// The recorded value is from replay_catchup_stall_skip_v12.json.gz, which main recorded (see
// TestGeneratePreV15CatchupFixtures) with the same defaults this test runs with.
func TestTweakablesUnchangedBelowChunkedCatchup(t *testing.T) {
	recorded := fixtureTweakables(t, "testdata/replay_catchup_stall_skip_v12.json.gz")
	require.EqualValues(t, TriggerImmediatelyTimestamp, recorded.Version)
	require.Zero(t, recorded.MaxCatchupMatchesPerTask, "main has no such field")

	withOneIteration(t)
	recorded.IterationsBeforeContinueAsNew = 1 // the one tweakable this test changes
	var computed []TweakablePolicies
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetStartTime(baseStartTime)
	env.SetWorkerOptions(sdkworker.Options{
		Interceptors: []interceptor.WorkerInterceptor{&mutableSideEffectCapture{values: &computed}},
	})
	// Not behind: the run sleeps once, re-evaluates the tweakables, and continues-as-new.
	env.ExecuteWorkflow(SchedulerWorkflow, newCatchupArgs(t, 0))
	require.True(t, env.IsWorkflowCompleted())

	require.Len(t, computed, 2, "evaluated at start and after the sleep")
	for _, c := range computed {
		require.Equal(t, recorded, c)
	}
}

// fixtureTweakables returns the first tweakables value recorded in a fixture.
func fixtureTweakables(t *testing.T, path string) TweakablePolicies {
	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	zr, err := gzip.NewReader(f)
	require.NoError(t, err)
	raw, err := io.ReadAll(zr)
	require.NoError(t, err)

	// The marker's value is a JSON payload inside a base64 protobuf payload, inside the
	// history JSON: decode the history and find the base64 values that hold it.
	var history struct {
		Events []struct {
			MarkerRecordedEventAttributes *struct {
				MarkerName string `json:"markerName"`
				Details    map[string]struct {
					Payloads []struct {
						Data []byte `json:"data"`
					} `json:"payloads"`
				} `json:"details"`
			} `json:"markerRecordedEventAttributes"`
		} `json:"events"`
	}
	require.NoError(t, json.Unmarshal(raw, &history))
	tweakablesJSON := regexp.MustCompile(`\{"DefaultCatchupWindow":[^}]*\}`)
	for _, e := range history.Events {
		m := e.MarkerRecordedEventAttributes
		if m == nil || m.MarkerName != "MutableSideEffect" {
			continue
		}
		for _, p := range m.Details["data"].Payloads {
			if js := tweakablesJSON.Find(p.Data); js != nil {
				var tw TweakablePolicies
				require.NoError(t, json.Unmarshal(js, &tw))
				return tw
			}
		}
	}
	require.FailNow(t, "no tweakables marker in "+path)
	return TweakablePolicies{}
}

// mutableSideEffectCapture records the value each MutableSideEffect callback computes.
type mutableSideEffectCapture struct {
	interceptor.WorkerInterceptorBase
	values *[]TweakablePolicies
}

func (c *mutableSideEffectCapture) InterceptWorkflow(_ workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &mutableSideEffectInbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}, values: c.values}
}

type mutableSideEffectInbound struct {
	interceptor.WorkflowInboundInterceptorBase
	values *[]TweakablePolicies
}

func (i *mutableSideEffectInbound) Init(outbound interceptor.WorkflowOutboundInterceptor) error {
	return i.Next.Init(&mutableSideEffectOutbound{WorkflowOutboundInterceptorBase: interceptor.WorkflowOutboundInterceptorBase{Next: outbound}, values: i.values})
}

type mutableSideEffectOutbound struct {
	interceptor.WorkflowOutboundInterceptorBase
	values *[]TweakablePolicies
}

func (o *mutableSideEffectOutbound) MutableSideEffect(
	ctx workflow.Context, id string, f func(ctx workflow.Context) any, equals func(a, b any) bool,
) converter.EncodedValue {
	return o.Next.MutableSideEffect(ctx, id, func(ctx workflow.Context) any {
		v := f(ctx)
		if tw, ok := v.(TweakablePolicies); ok {
			*o.values = append(*o.values, tw)
		}
		return v
	}, equals)
}
