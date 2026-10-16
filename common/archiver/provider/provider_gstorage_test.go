//go:build !temporal_minimal || temporal_with_gstorage

package provider

import (
	"go.temporal.io/server/common/archiver/gcloud"
	"go.temporal.io/server/common/config"
)

func (s *ProviderSuite) TestGetHistoryArchiver_ConfigNotFound_GCloud() {
	scheme := gcloud.URIScheme

	provider, err := NewArchiverProvider(
		&config.HistoryArchiverProvider{
			Gstorage: nil, // Config not set
		},
		nil,
		nil,
		nil,
		s.mockExecutionManager,
		s.logger,
		s.metricsHandler,
	)
	s.NoError(err)

	result, err := provider.GetHistoryArchiver(scheme)
	s.Error(err)
	s.Equal(ErrArchiverConfigNotFound, err)
	s.Nil(result)
}

func (s *ProviderSuite) TestGetVisibilityArchiver_ConfigNotFound_GCloud() {
	scheme := gcloud.URIScheme

	provider, err := NewArchiverProvider(
		nil,
		&config.VisibilityArchiverProvider{
			Gstorage: nil, // Config not set
		},
		nil,
		nil,
		s.mockExecutionManager,
		s.logger,
		s.metricsHandler,
	)
	s.NoError(err)

	result, err := provider.GetVisibilityArchiver(scheme)
	s.Error(err)
	s.Equal(ErrArchiverConfigNotFound, err)
	s.Nil(result)
}

func (s *ProviderSuite) TestNewArchiverProvider_Gstorage() {
	provider, err := NewArchiverProvider(
		&config.HistoryArchiverProvider{Gstorage: &config.GstorageArchiver{}},
		&config.VisibilityArchiverProvider{Gstorage: &config.GstorageArchiver{}},
		nil,
		nil,
		s.mockExecutionManager,
		s.logger,
		s.metricsHandler,
	)
	s.NoError(err)
	s.NotNil(provider)
}
