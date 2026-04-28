package tests

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/tests/model"
	"go.temporal.io/server/tests/model/namespace"
	"go.temporal.io/server/tests/model/nexusendpoint"
	"go.temporal.io/server/tests/model/standalonenexusop"
	"go.temporal.io/server/tests/model/taskqueue"
	"go.temporal.io/server/tests/model/world"
	"go.temporal.io/server/tests/testcore"
	"go.temporal.io/server/tests/testcore/umpire"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var nexusStandaloneOpts = []testcore.TestOption{
	testcore.WithDedicatedCluster(),
	testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
	testcore.WithDynamicConfig(nexusoperation.Enabled, true),
}

func isFallbackSafeRequestMutationError(err error) bool {
	var invalidArgumentErr *serviceerror.InvalidArgument
	var notFoundErr *serviceerror.NotFound
	var namespaceNotFoundErr *serviceerror.NamespaceNotFound
	var alreadyStartedErr *serviceerror.NexusOperationExecutionAlreadyStarted
	var cancellationAlreadyRequestedErr *serviceerror.CancellationAlreadyRequested
	if errors.As(err, &invalidArgumentErr) ||
		errors.As(err, &notFoundErr) ||
		errors.As(err, &namespaceNotFoundErr) ||
		errors.As(err, &alreadyStartedErr) ||
		errors.As(err, &cancellationAlreadyRequestedErr) {
		return true
	}
	switch status.Code(err) {
	case codes.InvalidArgument, codes.NotFound, codes.AlreadyExists, codes.FailedPrecondition:
		return true
	default:
		return false
	}
}

func nexusFaultPlan() map[string]umpire.MethodFault {
	delay := umpire.MethodFault{
		Delay:    umpire.Pct(25),
		MaxDelay: 200 * time.Millisecond,
	}
	return map[string]umpire.MethodFault{
		workflowservice.WorkflowService_DescribeNexusOperationExecution_FullMethodName: delay,
		workflowservice.WorkflowService_PollNexusTaskQueue_FullMethodName:              delay,
	}
}

var propIterCounter atomic.Int64

// TestNexusStandaloneExamples runs every rule's curated example tests
// without spinning up the property-test harness. Each rule's Examples
// closure registers t.Run subtests; the framework auto-parallels them.
//
// Adding a new example to a rule lights up here; the cluster setup is
// each example's own concern (typically newNexusTestEnv).
func TestNexusStandaloneExamples(t *testing.T) {
	standalonenexusop.RunExamples(t)
}

// TestNexusStandaloneProperties is the rapid-driven property test: it
// exercises the API surface against a live in-process cluster and checks
// every registered rule's Check function on the resulting umpire history.
func TestNexusStandaloneProperties(t *testing.T) {
	u := &umpire.Umpire{}
	env := newNexusTestEnv(t, false, nexusStandaloneOpts...)

	// One World shared across all rapid iterations. The World resets at the
	// top of each iteration but each EntityStore pointer stays valid, so the
	// observers built once below keep updating the right stores.
	w := world.New()

	// Build the per-entity components once. Each component contributes its
	// observer (server-side strategy), rules, RPC catalog, and observed
	// methods. Driver state is per-iteration; we rebuild only the Behaviors
	// each iteration with iteration-scoped prefixes.
	components := buildComponents(u, w, env, "init", "init", "init")

	if err := components.Validate(); err != nil {
		t.Fatalf("component wiring: %v", err)
	}

	registry := &umpire.RPCRegistry{}
	components.PopulateRegistry(registry)

	intercept := umpire.NewInterceptor(append(
		[]umpire.Strategy{umpire.ObserveStrategy(u)},
		append(
			components.Strategies(),
			umpire.FaultStrategy(nexusFaultPlan()),
			umpire.RPCMutationStrategy(
				umpire.NewRequestMutations(),
				registry,
				isFallbackSafeRequestMutationError,
			),
			umpire.RPCRegistryStrategy(
				u,
				registry,
				umpire.WithRPCRegistryMethodFilter(model.IsNexusWorkflowServiceMethod),
				umpire.WithUnregisteredRPCViolations(),
			),
		)...,
	)...)
	env.SetClientUnaryInterceptor(intercept.UnaryClient())
	env.SetServerUnaryInterceptor(intercept.UnaryServer())

	umpire.Check(t, func(pt *umpire.T) {
		prefix := fmt.Sprintf("r%d", propIterCounter.Add(1))
		taskQueue := "prop-test-tq-" + prefix
		endpointName := env.createNexusEndpoint(env.Context(), t, testcore.RandomizedNexusEndpoint(t.Name()), taskQueue).Spec.Name

		w.Reset()
		iter := buildComponents(u, w, env, prefix, endpointName, taskQueue)
		scenario := umpire.NewComposite(iter.Behaviors()...)
		runner := umpire.NewModel(u, scenario)
		runner.Run(pt)
	})
}

// buildComponents constructs the full set of per-entity components for a
// given prefix. The shared World stores keep state across iterations only
// in the sense that their pointers are stable; w.Reset clears them in place
// before each iteration.
func buildComponents(u *umpire.Umpire, w *world.World, env *NexusTestEnv, prefix, endpointName, taskQueue string) umpire.Components {
	deps := model.Deps{
		Umpire:    u,
		Context:   env.Context(),
		Client:    env.FrontendClient(),
		Operator:  env.OperatorClient(),
		Namespace: env.Namespace().String(),
		Prefix:    prefix,
	}
	return umpire.Components{
		namespace.New(namespace.Deps{Deps: deps, Store: w.Namespaces}),
		taskqueue.New(taskqueue.Deps{Deps: deps, Store: w.TaskQueues}),
		nexusendpoint.New(nexusendpoint.Deps{Deps: deps, Store: w.Endpoints}),
		standalonenexusop.New(standalonenexusop.Deps{
			Deps:         deps,
			Store:        w.Operations,
			EndpointName: endpointName,
			TaskQueue:    taskQueue,
		}),
	}
}
