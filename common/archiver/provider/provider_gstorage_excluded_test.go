//go:build temporal_minimal && !temporal_with_gstorage

package provider

import (
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/config"
)

func (s *ProviderSuite) TestNewArchiverProvider_GstorageExcluded() {
	for _, configs := range []struct {
		history    *config.HistoryArchiverProvider
		visibility *config.VisibilityArchiverProvider
	}{
		{history: &config.HistoryArchiverProvider{Gstorage: &config.GstorageArchiver{}}},
		{visibility: &config.VisibilityArchiverProvider{Gstorage: &config.GstorageArchiver{}}},
	} {
		provider, err := NewArchiverProvider(
			configs.history,
			configs.visibility,
			nil,
			nil,
			s.mockExecutionManager,
			s.logger,
			s.metricsHandler,
		)
		s.ErrorContains(err, "temporal_with_gstorage")
		s.Nil(provider)
	}
}

func (s *ProviderSuite) TestGetArchiver_GstorageExcluded() {
	// A namespace can have a gs:// archival URI even if the gstorage provider isn't configured.
	provider, err := NewArchiverProvider(
		&config.HistoryArchiverProvider{},
		&config.VisibilityArchiverProvider{},
		nil,
		nil,
		s.mockExecutionManager,
		s.logger,
		s.metricsHandler,
	)
	s.NoError(err)

	historyArchiver, err := provider.GetHistoryArchiver(archiver.URISchemeGstorage)
	s.ErrorIs(err, ErrUnknownScheme)
	s.ErrorContains(err, "temporal_with_gstorage")
	s.Nil(historyArchiver)

	visibilityArchiver, err := provider.GetVisibilityArchiver(archiver.URISchemeGstorage)
	s.ErrorIs(err, ErrUnknownScheme)
	s.ErrorContains(err, "temporal_with_gstorage")
	s.Nil(visibilityArchiver)
}
