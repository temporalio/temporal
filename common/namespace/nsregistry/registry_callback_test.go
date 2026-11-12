package nsregistry

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/testlogger"
)

type callbackKey string

func (k callbackKey) String() string { return string(k) }

func TestRegisterStateChangeCallbackKeyIdentity(t *testing.T) {
	t.Parallel()

	r := &registry{
		idToNamespace:  make(map[namespace.ID]*namespace.Namespace),
		nameToID:       make(map[namespace.Name]namespace.ID),
		metricsHandler: metrics.NoopMetricsHandler,
	}
	for _, id := range []string{"first", "second"} {
		ns := namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{Id: id, Name: id}, nil, "active")
		r.idToNamespace[ns.ID()] = ns
		r.nameToID[ns.Name()] = ns.ID()
	}
	firstKey, secondKey := callbackKey("same"), callbackKey("same")
	var calls [2]int
	r.RegisterStateChangeCallback(&firstKey, func(*namespace.Namespace, bool) { calls[0]++ })
	r.RegisterStateChangeCallback(&secondKey, func(*namespace.Namespace, bool) { calls[1]++ })
	require.Equal(t, [2]int{2, 2}, calls)

	require.NoError(t, r.processWatchEvent(&persistence.NamespaceWatchEvent{
		Type:        persistence.NamespaceWatchEventTypeDelete,
		NamespaceID: "first",
	}))
	require.Equal(t, [2]int{3, 3}, calls)

	r.UnregisterStateChangeCallback(&firstKey)
	require.NoError(t, r.processWatchEvent(&persistence.NamespaceWatchEvent{
		Type:        persistence.NamespaceWatchEventTypeDelete,
		NamespaceID: "second",
	}))
	require.Equal(t, [2]int{3, 4}, calls)
}

func TestRegisterStateChangeCallbackLogsKey(t *testing.T) {
	t.Parallel()

	logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
	capture := logger.StartCapture(tag.String("key", "callback"))
	r := &registry{
		idToNamespace:  map[namespace.ID]*namespace.Namespace{"id": {}},
		metricsHandler: metrics.NoopMetricsHandler,
		logger:         logger,
	}
	r.RegisterStateChangeCallback(callbackKey("callback"), func(*namespace.Namespace, bool) {
		timer := time.NewTimer(2 * slowCallbackDuration)
		defer timer.Stop()
		<-timer.C
	})

	capture.RequireContains(t, testlogger.CapturedLogPattern{
		Level:   testlogger.Warn,
		Message: "Namespace registry callback slow",
		Tags: map[string]any{
			"key":      "callback",
			"duration": testlogger.AnyTagValue,
		},
	})
}
