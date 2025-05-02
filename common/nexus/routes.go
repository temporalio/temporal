package nexus

import "go.temporal.io/server/common/routing"

type NamespaceAndTaskQueue struct {
	Namespace string
	TaskQueue string
}

var RouteDispatchNexusTaskByNamespaceAndTaskQueue = routing.NewBuilder[NamespaceAndTaskQueue]().
	Constant("namespaces").
	StringVariable("namespace", func(params *NamespaceAndTaskQueue) *string { return &params.Namespace }).
	Constant("task-queues").
	StringVariable("task_queue", func(params *NamespaceAndTaskQueue) *string { return &params.TaskQueue }).
	Constant("nexus-services").
	Build()

var RouteDispatchNexusTaskByEndpoint = routing.NewBuilder[string]().
	Constant("nexus", "endpoints").
	StringVariable("endpoint", func(endpoint *string) *string { return endpoint }).
	Constant("services").
	Build()

// RouteCompletionCallback is an HTTP route for completing a Nexus operation via callback.
var RouteCompletionCallback = routing.NewBuilder[string]().
	Constant("namespaces").
	StringVariable("namespace", func(namespace *string) *string { return namespace }).
	Constant("nexus", "callback").
	Build()
