package nexus_test

import (
	"fmt"

	"go.temporal.io/server/common/nexus"
)

func ExampleRouteDispatchNexusTaskByNamespaceAndTaskQueue() {
	path := nexus.RouteDispatchNexusTaskByNamespaceAndTaskQueue.
		Path(nexus.NamespaceAndTaskQueue{
			Namespace: "TEST-NAMESPACE",
			TaskQueue: "TEST-TASK-QUEUE",
		})
	fmt.Println(path)
	// Output: namespaces/TEST-NAMESPACE/task-queues/TEST-TASK-QUEUE/nexus-services
}

func ExampleRouteDispatchNexusTaskByEndpoint() {
	path := nexus.RouteDispatchNexusTaskByEndpoint.
		Path("TEST-ENDPOINT")
	fmt.Println(path)
	// Output: nexus/endpoints/TEST-ENDPOINT/services
}
