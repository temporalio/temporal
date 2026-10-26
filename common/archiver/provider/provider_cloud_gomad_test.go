//go:build gomad || gomad_wasm_slim

package provider

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

func TestCloudArchiversRefuseConfiguredSchemesWithoutFallback(t *testing.T) {
	provider := NewArchiverProvider(
		&config.HistoryArchiverProvider{Gstorage: &config.GstorageArchiver{}, S3store: &config.S3Archiver{}},
		&config.VisibilityArchiverProvider{Gstorage: &config.GstorageArchiver{}, S3store: &config.S3Archiver{}},
		nil,
		nil,
		nil,
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
	)
	for _, scheme := range []string{"gs", "s3"} {
		history, err := provider.GetHistoryArchiver(scheme)
		require.ErrorIs(t, err, errCloudArchiverUnavailable, scheme)
		require.Nil(t, history)
		visibility, err := provider.GetVisibilityArchiver(scheme)
		require.ErrorIs(t, err, errCloudArchiverUnavailable, scheme)
		require.Nil(t, visibility)
	}
	unconfigured := NewArchiverProvider(&config.HistoryArchiverProvider{}, &config.VisibilityArchiverProvider{}, nil, nil, nil, log.NewNoopLogger(), metrics.NoopMetricsHandler)
	_, err := unconfigured.GetHistoryArchiver("gs")
	require.ErrorIs(t, err, ErrArchiverConfigNotFound)
	_, err = unconfigured.GetVisibilityArchiver("s3")
	require.ErrorIs(t, err, ErrArchiverConfigNotFound)
}
