package frontend

import (
	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
)

func newOperationContext() *operationContext {
	oc := &operationContext{
		nexusContext: &nexusContext{},
	}
	oc.logger = log.NewTestLogger()
	oc.metricsHandlerForInterceptors = metricstest.NewCaptureHandler()
	oc.apiName = "/temporal.api.nexusservice.v1.NexusService/DispatchNexusTask"
	oc.responseHeaders = make(map[string]string)

	oc.namespaceName = "test-namespace"
	oc.namespace = namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{
			Id:    uuid.NewString(),
			Name:  oc.namespaceName,
			State: enumspb.NAMESPACE_STATE_REGISTERED,
		},
		&persistencespb.NamespaceConfig{
			Retention:                    timestamp.DurationFromDays(1),
			CustomSearchAttributeAliases: make(map[string]string),
		},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		1,
	)

	return oc
}
