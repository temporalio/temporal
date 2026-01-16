package mongodb

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/searchattribute"
)

type stubSAPProvider struct{}

type stubMapperProvider struct{}

func (stubSAPProvider) GetSearchAttributes(string, bool) (searchattribute.NameTypeMap, error) {
	return searchattribute.NameTypeMap{}, nil
}

func (stubMapperProvider) GetMapper(namespace.Name) (searchattribute.Mapper, error) {
	return nil, nil
}

func TestFactoryNewVisibilityStore(t *testing.T) {
	t.Parallel()

	db := newFakeVisibilityDatabase(t)
	executionsCol := newFakeVisibilityCollection(t)
	searchAttrCol := newFakeVisibilityCollection(t)

	db.collections[collectionVisibilityExecutions] = executionsCol
	db.collections[collectionVisibilitySearchAttributes] = searchAttrCol

	client := &fakeMongoClient{}
	logger := log.NewTestLogger()

	factory := &Factory{
		cfg:                 config.MongoDB{},
		client:              client,
		logger:              logger,
		database:            db,
		transactionsEnabled: true,
	}

	saProvider := &stubSAPProvider{}
	saMapperProvider := &stubMapperProvider{}
	chasmRegistry := chasm.NewRegistry(logger)

	visStore, err := factory.NewVisibilityStore(
		config.CustomDatastoreConfig{},
		saProvider,
		saMapperProvider,
		nil,
		chasmRegistry,
		nil,
		logger,
		metrics.NoopMetricsHandler,
	)
	require.NoError(t, err)
	require.NotNil(t, visStore)

	impl, ok := visStore.(*visibilityStore)
	require.True(t, ok)
	require.Equal(t, saProvider, impl.searchAttributesProvider)
	require.Equal(t, saMapperProvider, impl.searchAttributesMapperProvider)
	require.Equal(t, chasmRegistry, impl.chasmRegistry)

	// Visibility store is NOT cached because different services have different chasm registries.
	// Each call should return a new instance with the provided parameters.
	chasmRegistry2 := chasm.NewRegistry(logger)
	saProvider2 := &stubSAPProvider{}
	saMapperProvider2 := &stubMapperProvider{}

	visStore2, err := factory.NewVisibilityStore(
		config.CustomDatastoreConfig{},
		saProvider2,
		saMapperProvider2,
		nil,
		chasmRegistry2,
		nil,
		logger,
		metrics.NoopMetricsHandler,
	)
	require.NoError(t, err)
	require.NotSame(t, visStore, visStore2)

	impl2, ok := visStore2.(*visibilityStore)
	require.True(t, ok)
	require.Equal(t, saProvider2, impl2.searchAttributesProvider)
	require.Equal(t, saMapperProvider2, impl2.searchAttributesMapperProvider)
	require.Equal(t, chasmRegistry2, impl2.chasmRegistry)

	require.NotNil(t, executionsCol.indexView)
	require.NotNil(t, searchAttrCol.indexView)
}
