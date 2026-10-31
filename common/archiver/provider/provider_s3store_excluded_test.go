//go:build temporal_minimal && !temporal_with_s3store

package provider

import (
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/config"
)

func (s *ProviderSuite) TestGetArchiver_S3storeExcluded() {
	provider := NewArchiverProvider(
		&config.HistoryArchiverProvider{},
		&config.VisibilityArchiverProvider{},
		nil,
		nil,
		s.mockExecutionManager,
		s.logger,
		s.metricsHandler,
	)

	historyArchiver, err := provider.GetHistoryArchiver(archiver.URISchemeS3store)
	s.ErrorIs(err, ErrUnknownScheme)
	s.EqualError(err, `unknown archiver scheme "s3": S3 archival is not available on this server`)
	s.Nil(historyArchiver)

	visibilityArchiver, err := provider.GetVisibilityArchiver(archiver.URISchemeS3store)
	s.ErrorIs(err, ErrUnknownScheme)
	s.EqualError(err, `unknown archiver scheme "s3": S3 archival is not available on this server`)
	s.Nil(visibilityArchiver)
}
