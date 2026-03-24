package provider

import (
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/filestore"
	"go.temporal.io/server/common/archiver/gcloud"
	"go.temporal.io/server/common/archiver/s3store"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.uber.org/mock/gomock"
)

type (
	ProviderSuite struct {
		*require.Assertions
		suite.Suite

		controller                  *gomock.Controller
		mockExecutionManager        *persistence.MockExecutionManager
		mockHistoryArchiver         *archiver.MockHistoryArchiver
		mockVisibilityArchiver      *archiver.MockVisibilityArchiver
		mockCustomHistoryFactory    *MockCustomHistoryArchiverFactory
		mockCustomVisibilityFactory *MockCustomVisibilityArchiverFactory

		logger         log.Logger
		metricsHandler metrics.Handler
	}
)

func TestProviderSuite(t *testing.T) {
	suite.Run(t, new(ProviderSuite))
}

func (s *ProviderSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())

	s.mockExecutionManager = persistence.NewMockExecutionManager(s.controller)
	s.mockHistoryArchiver = archiver.NewMockHistoryArchiver(s.controller)
	s.mockVisibilityArchiver = archiver.NewMockVisibilityArchiver(s.controller)
	s.mockCustomHistoryFactory = NewMockCustomHistoryArchiverFactory(s.controller)
	s.mockCustomVisibilityFactory = NewMockCustomVisibilityArchiverFactory(s.controller)

	s.logger = log.NewNoopLogger()
	s.metricsHandler = metrics.NoopMetricsHandler
}

func (s *ProviderSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *ProviderSuite) TestNewArchiverProvider() {
	historyConfig := &config.HistoryArchiverProvider{}
	visibilityConfig := &config.VisibilityArchiverProvider{}

	provider := NewArchiverProvider(
		historyConfig,
		visibilityConfig,
		s.mockCustomHistoryFactory,
		s.mockCustomVisibilityFactory,
		s.mockExecutionManager,
		s.logger,
		s.metricsHandler,
	)

	s.NotNil(provider)

	// Verify internal state
	p := provider.(*archiverProvider)
	s.Equal(historyConfig, p.historyArchiverConfigs)
	s.Equal(visibilityConfig, p.visibilityArchiverConfigs)
	s.Equal(s.mockCustomHistoryFactory, p.customHistoryArchiverFactory)
	s.Equal(s.mockCustomVisibilityFactory, p.customVisibilityArchiverFactory)
	s.Equal(s.mockExecutionManager, p.executionManager)
	s.Equal(s.logger, p.logger)
	s.Equal(s.metricsHandler, p.metricsHandler)
	s.NotNil(p.historyArchivers)
	s.NotNil(p.visibilityArchivers)
}

func (s *ProviderSuite) TestGetHistoryArchiver_CustomFactory_Success() {
	scheme := "custom"

	provider := NewArchiverProvider(
		&config.HistoryArchiverProvider{
			CustomStores: map[string]map[string]any{
				scheme: {"key": "value"},
			},
		},
		nil,
		s.mockCustomHistoryFactory,
		nil,
		s.mockExecutionManager,
		s.logger,
		s.metricsHandler,
	)

	s.mockCustomHistoryFactory.EXPECT().
		NewCustomHistoryArchiver(gomock.Any()).
		DoAndReturn(func(params NewCustomHistoryArchiverParams) (archiver.HistoryArchiver, error) {
			s.Equal(scheme, params.Scheme)
			s.Equal(s.mockExecutionManager, params.ExecutionManager)
			s.Equal(s.logger, params.Logger)
			s.Equal(s.metricsHandler, params.MetricsHandler)
			s.Equal(map[string]any{"key": "value"}, params.Configs)
			return s.mockHistoryArchiver, nil
		})

	result, err := provider.GetHistoryArchiver(scheme)
	s.NoError(err)
	s.Equal(s.mockHistoryArchiver, result)

	// Test caching - should not call factory again
	result2, err := provider.GetHistoryArchiver(scheme)
	s.NoError(err)
	s.Equal(result, result2)
}

func (s *ProviderSuite) TestGetHistoryArchiver_CustomFactory_ReturnsError() {
	scheme := "custom"
	expectedErr := errors.New("custom factory error")

	provider := NewArchiverProvider(
		nil,
		nil,
		s.mockCustomHistoryFactory,
		nil,
		s.mockExecutionManager,
		s.logger,
		s.metricsHandler,
	)

	s.mockCustomHistoryFactory.EXPECT().
		NewCustomHistoryArchiver(gomock.Any()).
		Return(nil, expectedErr)

	result, err := provider.GetHistoryArchiver(scheme)
	s.Error(err)
	s.Equal(expectedErr, err)
	s.Nil(result)
}

func (s *ProviderSuite) TestGetHistoryArchiver_CustomFactory_FallbackToBuiltIn() {
	scheme := filestore.URIScheme

	provider := NewArchiverProvider(
		&config.HistoryArchiverProvider{
			Filestore: &config.FilestoreArchiver{
				FileMode: "0600",
				DirMode:  "0700",
			},
		},
		nil,
		s.mockCustomHistoryFactory,
		nil,
		s.mockExecutionManager,
		s.logger,
		s.metricsHandler,
	)

	// Custom factory returns ErrUnknownScheme, should fallback to built-in
	s.mockCustomHistoryFactory.EXPECT().
		NewCustomHistoryArchiver(gomock.Any()).
		Return(nil, ErrUnknownScheme)

	result, err := provider.GetHistoryArchiver(scheme)
	s.NoError(err)
	s.NotNil(result)

	// Verify it's cached
	result2, err := provider.GetHistoryArchiver(scheme)
	s.NoError(err)
	s.Equal(result, result2)
}

func (s *ProviderSuite) TestGetHistoryArchiver_UnknownScheme() {
	scheme := "unknown"

	provider := NewArchiverProvider(
		nil,
		nil,
		nil,
		nil,
		s.mockExecutionManager,
		s.logger,
		s.metricsHandler,
	)

	result, err := provider.GetHistoryArchiver(scheme)
	s.Error(err)
	s.Equal(ErrUnknownScheme, err)
	s.Nil(result)
}

func (s *ProviderSuite) TestGetHistoryArchiver_ConfigNotFound_Filestore() {
	scheme := filestore.URIScheme

	provider := NewArchiverProvider(
		&config.HistoryArchiverProvider{
			Filestore: nil, // Config not set
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

func (s *ProviderSuite) TestGetHistoryArchiver_ConfigNotFound_GCloud() {
	scheme := gcloud.URIScheme

	provider := NewArchiverProvider(
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

	result, err := provider.GetHistoryArchiver(scheme)
	s.Error(err)
	s.Equal(ErrArchiverConfigNotFound, err)
	s.Nil(result)
}

func (s *ProviderSuite) TestGetHistoryArchiver_ConfigNotFound_S3() {
	scheme := s3store.URIScheme

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

func (s *ProviderSuite) TestGetHistoryArchiver_ConcurrentAccess() {
	scheme := "custom"

	provider := NewArchiverProvider(
		nil,
		nil,
		s.mockCustomHistoryFactory,
		nil,
		s.mockExecutionManager,
		s.logger,
		s.metricsHandler,
	)

	// The factory may be called multiple times before caching occurs,
	// but all calls should return the same archiver.
	s.mockCustomHistoryFactory.EXPECT().
		NewCustomHistoryArchiver(gomock.Any()).
		Return(s.mockHistoryArchiver, nil).
		AnyTimes()

	var wg sync.WaitGroup
	numGoroutines := 10
	results := make([]archiver.HistoryArchiver, numGoroutines)
	archiverErrors := make([]error, numGoroutines)

	for i := range numGoroutines {
		wg.Go(func() {
			results[i], archiverErrors[i] = provider.GetHistoryArchiver(scheme)
		})
	}

	wg.Wait()

	// All should succeed and return a valid archiver
	for i := 0; i < numGoroutines; i++ {
		s.NoError(archiverErrors[i])
		s.NotNil(results[i])
	}

	// Verify caching works after concurrent initialization
	cachedResult, err := provider.GetHistoryArchiver(scheme)
	s.NoError(err)
	s.NotNil(cachedResult)
}

func (s *ProviderSuite) TestGetHistoryArchiver_NilConfigs() {
	scheme := "custom"

	provider := NewArchiverProvider(
		nil, // nil history config
		nil,
		s.mockCustomHistoryFactory,
		nil,
		s.mockExecutionManager,
		s.logger,
		s.metricsHandler,
	)

	s.mockCustomHistoryFactory.EXPECT().
		NewCustomHistoryArchiver(gomock.Any()).
		DoAndReturn(func(params NewCustomHistoryArchiverParams) (archiver.HistoryArchiver, error) {
			s.Nil(params.Configs) // Should be nil when configs are not provided
			return s.mockHistoryArchiver, nil
		})

	result, err := provider.GetHistoryArchiver(scheme)
	s.NoError(err)
	s.Equal(s.mockHistoryArchiver, result)
}

func (s *ProviderSuite) TestCustomHistoryArchiverFactoryFunc() {
	called := false
	expectedArchiver := s.mockHistoryArchiver

	factoryFunc := CustomHistoryArchiverFactoryFunc(func(params NewCustomHistoryArchiverParams) (archiver.HistoryArchiver, error) {
		called = true
		s.Equal("test-scheme", params.Scheme)
		return expectedArchiver, nil
	})

	result, err := factoryFunc.NewCustomHistoryArchiver(NewCustomHistoryArchiverParams{
		Scheme: "test-scheme",
	})

	s.NoError(err)
	s.True(called)
	s.Equal(expectedArchiver, result)
}

func (s *ProviderSuite) TestGetVisibilityArchiver_CustomFactory_Success() {
	scheme := "custom"

	provider := NewArchiverProvider(
		nil,
		&config.VisibilityArchiverProvider{
			CustomStores: map[string]map[string]any{
				scheme: {"key": "value"},
			},
		},
		nil,
		s.mockCustomVisibilityFactory,
		s.mockExecutionManager,
		s.logger,
		s.metricsHandler,
	)

	s.mockCustomVisibilityFactory.EXPECT().
		NewCustomVisibilityArchiver(gomock.Any()).
		DoAndReturn(func(params NewCustomVisibilityArchiverParams) (archiver.VisibilityArchiver, error) {
			s.Equal(scheme, params.Scheme)
			s.Equal(s.logger, params.Logger)
			s.Equal(s.metricsHandler, params.MetricsHandler)
			s.Equal(map[string]any{"key": "value"}, params.Configs)
			return s.mockVisibilityArchiver, nil
		})

	result, err := provider.GetVisibilityArchiver(scheme)
	s.NoError(err)
	s.Equal(s.mockVisibilityArchiver, result)

	// Test caching - should not call factory again
	result2, err := provider.GetVisibilityArchiver(scheme)
	s.NoError(err)
	s.Equal(result, result2)
}

func (s *ProviderSuite) TestGetVisibilityArchiver_CustomFactory_ReturnsError() {
	scheme := "custom"
	expectedErr := errors.New("custom factory error")

	provider := NewArchiverProvider(
		nil,
		nil,
		nil,
		s.mockCustomVisibilityFactory,
		s.mockExecutionManager,
		s.logger,
		s.metricsHandler,
	)

	s.mockCustomVisibilityFactory.EXPECT().
		NewCustomVisibilityArchiver(gomock.Any()).
		Return(nil, expectedErr)

	result, err := provider.GetVisibilityArchiver(scheme)
	s.Error(err)
	s.Equal(expectedErr, err)
	s.Nil(result)
}

func (s *ProviderSuite) TestGetVisibilityArchiver_CustomFactory_FallbackToBuiltIn() {
	scheme := filestore.URIScheme

	provider := NewArchiverProvider(
		nil,
		&config.VisibilityArchiverProvider{
			Filestore: &config.FilestoreArchiver{
				FileMode: "0600",
				DirMode:  "0700",
			},
		},
		nil,
		s.mockCustomVisibilityFactory,
		s.mockExecutionManager,
		s.logger,
		s.metricsHandler,
	)

	// Custom factory returns ErrUnknownScheme, should fallback to built-in
	s.mockCustomVisibilityFactory.EXPECT().
		NewCustomVisibilityArchiver(gomock.Any()).
		Return(nil, ErrUnknownScheme)

	result, err := provider.GetVisibilityArchiver(scheme)
	s.NoError(err)
	s.NotNil(result)

	// Verify it's cached
	result2, err := provider.GetVisibilityArchiver(scheme)
	s.NoError(err)
	s.Equal(result, result2)
}

func (s *ProviderSuite) TestGetVisibilityArchiver_UnknownScheme() {
	scheme := "unknown"

	provider := NewArchiverProvider(
		nil,
		nil,
		nil,
		nil,
		s.mockExecutionManager,
		s.logger,
		s.metricsHandler,
	)

	result, err := provider.GetVisibilityArchiver(scheme)
	s.Error(err)
	s.Equal(ErrUnknownScheme, err)
	s.Nil(result)
}

func (s *ProviderSuite) TestGetVisibilityArchiver_ConfigNotFound_Filestore() {
	scheme := filestore.URIScheme

	provider := NewArchiverProvider(
		nil,
		&config.VisibilityArchiverProvider{
			Filestore: nil, // Config not set
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

func (s *ProviderSuite) TestGetVisibilityArchiver_ConfigNotFound_GCloud() {
	scheme := gcloud.URIScheme

	provider := NewArchiverProvider(
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

	result, err := provider.GetVisibilityArchiver(scheme)
	s.Error(err)
	s.Equal(ErrArchiverConfigNotFound, err)
	s.Nil(result)
}

func (s *ProviderSuite) TestGetVisibilityArchiver_ConfigNotFound_S3() {
	scheme := s3store.URIScheme

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

func (s *ProviderSuite) TestGetVisibilityArchiver_ConcurrentAccess() {
	scheme := "custom"

	provider := NewArchiverProvider(
		nil,
		nil,
		nil,
		s.mockCustomVisibilityFactory,
		s.mockExecutionManager,
		s.logger,
		s.metricsHandler,
	)

	// The factory may be called multiple times before caching occurs,
	// but all calls should return the same archiver.
	s.mockCustomVisibilityFactory.EXPECT().
		NewCustomVisibilityArchiver(gomock.Any()).
		Return(s.mockVisibilityArchiver, nil).
		AnyTimes()

	var wg sync.WaitGroup
	numGoroutines := 10
	results := make([]archiver.VisibilityArchiver, numGoroutines)
	archiverErrors := make([]error, numGoroutines)

	for i := range numGoroutines {
		wg.Go(func() {
			results[i], archiverErrors[i] = provider.GetVisibilityArchiver(scheme)
		})
	}

	wg.Wait()

	// All should succeed and return a valid archiver
	for i := 0; i < numGoroutines; i++ {
		s.NoError(archiverErrors[i])
		s.NotNil(results[i])
	}

	// Verify caching works after concurrent initialization
	cachedResult, err := provider.GetVisibilityArchiver(scheme)
	s.NoError(err)
	s.NotNil(cachedResult)
}

func (s *ProviderSuite) TestGetVisibilityArchiver_NilConfigs() {
	scheme := "custom"

	provider := NewArchiverProvider(
		nil,
		nil, // nil visibility config
		nil,
		s.mockCustomVisibilityFactory,
		s.mockExecutionManager,
		s.logger,
		s.metricsHandler,
	)

	s.mockCustomVisibilityFactory.EXPECT().
		NewCustomVisibilityArchiver(gomock.Any()).
		DoAndReturn(func(params NewCustomVisibilityArchiverParams) (archiver.VisibilityArchiver, error) {
			s.Nil(params.Configs) // Should be nil when configs are not provided
			return s.mockVisibilityArchiver, nil
		})

	result, err := provider.GetVisibilityArchiver(scheme)
	s.NoError(err)
	s.Equal(s.mockVisibilityArchiver, result)
}

func (s *ProviderSuite) TestCustomVisibilityArchiverFactoryFunc() {
	called := false
	expectedArchiver := s.mockVisibilityArchiver

	factoryFunc := CustomVisibilityArchiverFactoryFunc(func(params NewCustomVisibilityArchiverParams) (archiver.VisibilityArchiver, error) {
		called = true
		s.Equal("test-scheme", params.Scheme)
		return expectedArchiver, nil
	})

	result, err := factoryFunc.NewCustomVisibilityArchiver(NewCustomVisibilityArchiverParams{
		Scheme: "test-scheme",
	})

	s.NoError(err)
	s.True(called)
	s.Equal(expectedArchiver, result)
}
