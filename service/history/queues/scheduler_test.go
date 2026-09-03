package queues

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.uber.org/mock/gomock"
)

func TestNamespaceStateFilterScheduler_Submit(t *testing.T) {
	for _, tc := range []struct {
		name          string
		namespace     *namespace.Namespace
		namespaceErr  error
		expectDiscard bool
	}{
		{
			name: "deleted namespace",
			namespace: namespace.NewLocalNamespaceForTest(
				&persistencespb.NamespaceInfo{State: enumspb.NAMESPACE_STATE_DELETED},
				nil,
				"",
			),
			expectDiscard: true,
		},
		{
			name: "registered namespace",
			namespace: namespace.NewLocalNamespaceForTest(
				&persistencespb.NamespaceInfo{State: enumspb.NAMESPACE_STATE_REGISTERED},
				nil,
				"",
			),
		},
		{
			name:         "namespace lookup error",
			namespaceErr: serviceerror.NewNamespaceNotFound("namespace"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			controller := gomock.NewController(t)
			baseScheduler := NewMockScheduler(controller)
			namespaceRegistry := namespace.NewMockRegistry(controller)
			executable := NewMockExecutable(controller)

			executable.EXPECT().GetNamespaceID().Return("namespace-id")
			namespaceRegistry.EXPECT().GetNamespaceByID(namespace.ID("namespace-id")).
				Return(tc.namespace, tc.namespaceErr)
			if tc.expectDiscard {
				executable.EXPECT().Ack()
			} else {
				baseScheduler.EXPECT().Submit(executable)
			}

			scheduler := &namespaceStateFilterScheduler{
				Scheduler:         baseScheduler,
				namespaceRegistry: namespaceRegistry,
				metricsHandler:    metrics.NoopMetricsHandler,
				metricTagsFn:      func(Executable) []metrics.Tag { return nil },
			}
			scheduler.Submit(executable)
		})
	}
}

func TestNamespaceStateFilterScheduler_TrySubmitDeletedNamespace(t *testing.T) {
	controller := gomock.NewController(t)
	baseScheduler := NewMockScheduler(controller)
	namespaceRegistry := namespace.NewMockRegistry(controller)
	executable := NewMockExecutable(controller)

	executable.EXPECT().GetNamespaceID().Return("namespace-id")
	namespaceRegistry.EXPECT().GetNamespaceByID(namespace.ID("namespace-id")).Return(
		namespace.NewLocalNamespaceForTest(
			&persistencespb.NamespaceInfo{State: enumspb.NAMESPACE_STATE_DELETED},
			nil,
			"",
		),
		nil,
	)
	executable.EXPECT().Ack()

	scheduler := &namespaceStateFilterScheduler{
		Scheduler:         baseScheduler,
		namespaceRegistry: namespaceRegistry,
		metricsHandler:    metrics.NoopMetricsHandler,
		metricTagsFn:      func(Executable) []metrics.Tag { return nil },
	}
	require.True(t, scheduler.TrySubmit(executable))
}
