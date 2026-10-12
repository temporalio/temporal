//go:build temporal_minimal && !temporal_with_elasticsearch

package elasticsearch

import (
	"errors"

	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/searchattribute"
)

// VisibilityStore is a placeholder: Elasticsearch support is compiled out of this build.
type VisibilityStore struct {
	store.VisibilityStore
}

func NewVisibilityStore(
	_ *client.Config,
	_ *ProcessorConfig,
	_ searchattribute.Provider,
	_ searchattribute.MapperProvider,
	_ *chasm.Registry,
	_ dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	_ dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	_ metrics.Handler,
	_ log.Logger,
) (*VisibilityStore, error) {
	return nil, errors.New("elasticsearch visibility is excluded from this build, see build tag temporal_with_elasticsearch")
}
