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

// Environment exposes the functional-test capabilities used by action realizers and oracles.
type Environment interface {
	Namespace() namespace.Name
	NamespaceID() namespace.ID
	FrontendClient() workflowservice.WorkflowServiceClient
	SdkClient() sdkclient.Client
	SdkWorker() sdkworker.Worker
	WorkerTaskQueue() string
	InjectHook(testhooks.Hook) func()
	GetMonitor() testmonitor.Monitor
	GetFaultInjector() *faultinjection.RPCFaultGenerator
}
