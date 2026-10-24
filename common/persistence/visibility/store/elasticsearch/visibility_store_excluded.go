//go:build temporal_minimal && !temporal_with_elasticsearch

package elasticsearch

import (
	"errors"

	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/searchattribute"
)

// VisibilityStore is a placeholder: Elasticsearch support is compiled out of this build.
type VisibilityStore struct {
	store.VisibilityStore
}

// NewVisibilityStore fails, since Elasticsearch visibility is excluded from this build.
func NewVisibilityStore(
	_ *client.Config,
	_ *ProcessorConfig,
	_ searchattribute.Provider,
	_ searchattribute.MapperProvider,
	_ namespace.Registry,
	_ *chasm.Registry,
	_ dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	_ dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	_ metrics.Handler,
	_ log.Logger,
) (*VisibilityStore, error) {
	return nil, errors.New("elasticsearch visibility is not compiled into this server; rebuild with the temporal_with_elasticsearch build tag")
}
