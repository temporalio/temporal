//go:build temporal_minimal && !temporal_with_gstorage

package provider

import (
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/config"
)

func (s *ProviderSuite) TestGetArchiver_GstorageExcluded() {
	// A namespace can have a gs:// archival URI even if the gstorage provider isn't configured.
	provider := NewArchiverProvider(
		&config.HistoryArchiverProvider{},
		&config.VisibilityArchiverProvider{},
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
