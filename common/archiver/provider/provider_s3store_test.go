//go:build !temporal_minimal || temporal_with_s3store

package provider

import (
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/config"
)

func (s *ProviderSuite) TestGetHistoryArchiver_ConfigNotFound_S3() {
	scheme := archiver.URISchemeS3store

	provider := NewArchiverProvider(
		&config.HistoryArchiverProvider{
			S3store: nil, // Config not set
		},
		nil,
		nil,
		nil,
		s.mockExecutionManager,
		s.logger,
		s.metricsHandler,
	)

	result, err := provider.GetHistoryArchiver(scheme)
	s.Error(err)
	s.Equal(ErrArchiverConfigNotFound, err)
	s.Nil(result)
}

func (s *ProviderSuite) TestGetVisibilityArchiver_ConfigNotFound_S3() {
	scheme := archiver.URISchemeS3store

	provider := NewArchiverProvider(
		nil,
		&config.VisibilityArchiverProvider{
			S3store: nil, // Config not set
		},
		nil,
		nil,
		s.mockExecutionManager,
		s.logger,
		s.metricsHandler,
	)

	result, err := provider.GetVisibilityArchiver(scheme)
	s.Error(err)
	s.Equal(ErrArchiverConfigNotFound, err)
	s.Nil(result)
}
