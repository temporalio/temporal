//go:build temporal_minimal && !temporal_with_gstorage

package provider

import (
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/config"
)

func (s *ProviderSuite) TestGetArchiver_GstorageExcluded() {
	provider := NewArchiverProvider(
		&config.HistoryArchiverProvider{Gstorage: &config.GstorageArchiver{}},
		&config.VisibilityArchiverProvider{Gstorage: &config.GstorageArchiver{}},
		nil,
		nil,
		s.mockExecutionManager,
		s.logger,
		s.metricsHandler,
	)

	historyArchiver, err := provider.GetHistoryArchiver(archiver.URISchemeGstorage)
	s.ErrorIs(err, ErrUnknownScheme)
	s.ErrorContains(err, "temporal_with_gstorage")
	s.Nil(historyArchiver)

	visibilityArchiver, err := provider.GetVisibilityArchiver(archiver.URISchemeGstorage)
	s.ErrorIs(err, ErrUnknownScheme)
	s.ErrorContains(err, "temporal_with_gstorage")
	s.Nil(visibilityArchiver)
}
