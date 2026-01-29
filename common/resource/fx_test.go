package resource

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	archiverspb "go.temporal.io/server/api/archiver/v1"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/filestore"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/searchattribute"
)

type testHistoryArchiver struct{}

func (t *testHistoryArchiver) Archive(context.Context, archiver.URI, *archiver.ArchiveHistoryRequest, ...archiver.ArchiveOption) error {
	return nil
}

func (t *testHistoryArchiver) Get(context.Context, archiver.URI, *archiver.GetHistoryRequest) (*archiver.GetHistoryResponse, error) {
	return nil, nil
}

func (t *testHistoryArchiver) ValidateURI(archiver.URI) error {
	return nil
}

type testVisibilityArchiver struct{}

func (t *testVisibilityArchiver) Archive(context.Context, archiver.URI, *archiverspb.VisibilityRecord, ...archiver.ArchiveOption) error {
	return nil
}

func (t *testVisibilityArchiver) Query(context.Context, archiver.URI, *archiver.QueryVisibilityRequest, searchattribute.NameTypeMap) (*archiver.QueryVisibilityResponse, error) {
	return nil, nil
}

func (t *testVisibilityArchiver) ValidateURI(archiver.URI) error {
	return nil
}

func TestArchiverProviderProviderUsesFactory(t *testing.T) {
	historyCfg := &config.HistoryArchiverProvider{}
	visibilityCfg := &config.VisibilityArchiverProvider{}
	cfg := &config.Config{
		Archival: config.Archival{
			History:    config.HistoryArchival{Provider: historyCfg},
			Visibility: config.VisibilityArchival{Provider: visibilityCfg},
		},
	}

	historyArchiver := &testHistoryArchiver{}
	visibilityArchiver := &testVisibilityArchiver{}
	historyCalled := false
	visibilityCalled := false
	expectedScheme := filestore.URIScheme
	historyFactory := provider.CustomHistoryArchiverFactoryFunc(func(
		params provider.NewCustomHistoryArchiverParams,
	) (archiver.HistoryArchiver, error) {
		historyCalled = true
		require.Equal(t, expectedScheme, params.Scheme)
		return historyArchiver, nil
	})
	visibilityFactory := provider.CustomVisibilityArchiverFactoryFunc(func(
		params provider.NewCustomVisibilityArchiverParams,
	) (archiver.VisibilityArchiver, error) {
		visibilityCalled = true
		require.Equal(t, expectedScheme, params.Scheme)
		return visibilityArchiver, nil
	})

	archiverProvider := ArchiverProviderProvider(
		cfg,
		historyFactory,
		visibilityFactory,
		nil,
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
	)

	gotHistory, err := archiverProvider.GetHistoryArchiver(expectedScheme)
	require.NoError(t, err)
	require.True(t, historyCalled)
	require.Same(t, historyArchiver, gotHistory)

	gotVisibility, err := archiverProvider.GetVisibilityArchiver(expectedScheme)
	require.NoError(t, err)
	require.True(t, visibilityCalled)
	require.Same(t, visibilityArchiver, gotVisibility)
}
