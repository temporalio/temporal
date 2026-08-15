package action

import (
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/rpc/faultinjection"
	"go.temporal.io/server/common/testing/testhooks"
	testmonitor "go.temporal.io/server/tests/testcore/monitor"
)

// ScopedEnvironment identifies the isolated Temporal scope used by one path.
type ScopedEnvironment interface {
	Namespace() namespace.Name
	NamespaceID() namespace.ID
}

// PublicAPIEnvironment drives behavior through supported clients.
type PublicAPIEnvironment interface {
	FrontendClient() workflowservice.WorkflowServiceClient
	SdkClient() sdkclient.Client
}

// ParticipantEnvironment supplies environment-appropriate programmable participants.
type ParticipantEnvironment interface {
	SdkWorker() sdkworker.Worker
	WorkerTaskQueue() string
}

// InProcessObservationEnvironment exposes white-box facts when the selected profile permits them.
type InProcessObservationEnvironment interface {
	GetMonitor() testmonitor.Monitor
}

// FaultEnvironment exposes local fault controls; it does not confer deployment or canary authority.
type FaultEnvironment interface {
	InjectHook(testhooks.Hook) func()
	GetFaultInjector() *faultinjection.RPCFaultGenerator
}

// Environment exposes the functional-test capabilities used by action realizers and oracles.
// It composes capability-owned drivers used by the local functional-test realization.
type Environment interface {
	ScopedEnvironment
	PublicAPIEnvironment
	ParticipantEnvironment
	InProcessObservationEnvironment
	FaultEnvironment
}
