package deletenamespace

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/embedded"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/wideevents"
	"go.uber.org/mock/gomock"
)

// captureEventLogger records emitted wide-event records for assertions.
type captureEventLogger struct {
	embedded.Logger
	records []otellog.Record
}

func (c *captureEventLogger) Emit(_ context.Context, r otellog.Record) {
	c.records = append(c.records, r)
}
func (c *captureEventLogger) Enabled(context.Context, otellog.EnabledParameters) bool {
	return true
}

func attrString(rec otellog.Record, key string) string {
	var out string
	rec.WalkAttributes(func(kv otellog.KeyValue) bool {
		if kv.Key == key {
			out = kv.Value.AsString()
		}
		return true
	})
	return out
}

// Renaming a namespace to its tombstone name emits namespace_renamed with the original name/id.
func Test_RenameNamespaceActivity_EmitsNamespaceRenamed(t *testing.T) {
	ctrl := gomock.NewController(t)
	metadataManager := persistence.NewMockMetadataManager(ctrl)
	lg := &captureEventLogger{}

	a := &localActivities{
		metadataManager:              metadataManager,
		logger:                       log.NewTestLogger(),
		eventLogger:                  lg,
		emitNamespaceLifecycleEvents: dynamicconfig.GetBoolPropertyFn(true),
	}

	metadataManager.EXPECT().RenameNamespace(gomock.Any(), &persistence.RenameNamespaceRequest{
		PreviousName: "namespace",
		NewName:      "namespace-deleted-xyz",
	}).Return(nil)

	require.NoError(t, a.RenameNamespaceActivity(context.Background(), "namespace-id", "namespace", "namespace-deleted-xyz"))

	require.Len(t, lg.records, 1)
	require.Equal(t, wideevents.PhaseNamespaceRenamed, attrString(lg.records[0], "phase"))
	require.Equal(t, "namespace", attrString(lg.records[0], "namespace"))
	require.Equal(t, "namespace-id", attrString(lg.records[0], "namespace_id"))
}

func Test_RenameNamespaceActivity_EventDisabled(t *testing.T) {
	ctrl := gomock.NewController(t)
	metadataManager := persistence.NewMockMetadataManager(ctrl)
	lg := &captureEventLogger{}
	a := &localActivities{
		metadataManager:              metadataManager,
		logger:                       log.NewTestLogger(),
		eventLogger:                  lg,
		emitNamespaceLifecycleEvents: dynamicconfig.GetBoolPropertyFn(false),
	}

	metadataManager.EXPECT().RenameNamespace(gomock.Any(), gomock.Any()).Return(nil)
	require.NoError(t, a.RenameNamespaceActivity(context.Background(), "namespace-id", "namespace", "namespace-deleted-xyz"))
	require.Empty(t, lg.records)
}

// A no-op rename (new == previous) neither renames nor emits.
func Test_RenameNamespaceActivity_NoopEmitsNothing(t *testing.T) {
	lg := &captureEventLogger{}
	a := &localActivities{eventLogger: lg}

	require.NoError(t, a.RenameNamespaceActivity(context.Background(), "namespace-id", "namespace", "namespace"))
	require.Empty(t, lg.records)
}
