package history

import (
	"context"
	"sync"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence/visibility"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/esconfig"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.uber.org/fx"
)

const registerPreallocatedSearchAttributesAttemptTimeout = 30 * time.Second

var registerPreallocatedSearchAttributesRetryPolicy = backoff.NewExponentialRetryPolicy(time.Second).
	WithMaximumInterval(time.Minute).
	WithExpirationInterval(10 * time.Minute)

// RegisterPreallocatedSearchAttributeMappings adds the pre-allocated custom search attribute
// fields (eg: Keyword01) listed in cluster metadata to the mappings of the Elasticsearch
// visibility indexes. The index mapping is not dynamic, so values of unmapped fields are not
// indexed, and queries on their namespace aliases return nothing.
// It runs in the background when the service starts, and retries failures for a limited time.
func RegisterPreallocatedSearchAttributeMappings(
	lc fx.Lifecycle,
	visibilityManager manager.VisibilityManager,
	searchAttributesProvider searchattribute.Provider,
	logger log.Logger,
) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			wg.Go(func() {
				registerPreallocatedSearchAttributeMappings(
					ctx,
					visibilityManager,
					searchAttributesProvider,
					registerPreallocatedSearchAttributesRetryPolicy,
					logger,
				)
			})
			return nil
		},
		OnStop: func(context.Context) error {
			cancel()
			wg.Wait()
			return nil
		},
	})
}

func registerPreallocatedSearchAttributeMappings(
	ctx context.Context,
	visibilityManager manager.VisibilityManager,
	searchAttributesProvider searchattribute.Provider,
	retryPolicy backoff.RetryPolicy,
	logger log.Logger,
) {
	visibilityManagers := []manager.VisibilityManager{visibilityManager}
	if visibilityManagerDual, ok := visibilityManager.(*visibility.VisibilityManagerDual); ok {
		visibilityManagers = []manager.VisibilityManager{
			visibilityManagerDual.GetPrimaryVisibility(),
			visibilityManagerDual.GetSecondaryVisibility(),
		}
	}

	for _, visManager := range visibilityManagers {
		// TODO: Other visibility stores don't support adding search attributes.
		if visManager.GetStoreNames()[0] != esconfig.PersistenceName {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), registerPreallocatedSearchAttributesTimeout)
		defer cancel()
		indexName := visManager.GetIndexName()
		err := backoff.ThrottleRetryContext(
			ctx,
			func(ctx context.Context) error {
				return registerPreallocatedFieldMappings(ctx, visManager, searchAttributesProvider)
			},
			retryPolicy,
			nil,
		)
		if err != nil {
			logger.Error("Failed to register pre-allocated custom search attribute mappings.",
				tag.ESIndex(indexName), tag.Error(err))
			continue
		}
		logger.Info("Registered pre-allocated custom search attribute mappings.", tag.ESIndex(indexName))
	}
}

// registerPreallocatedFieldMappings adds the mappings of all the pre-allocated fields in one
// request. Adding a field that is already mapped with the same type doesn't change it.
func registerPreallocatedFieldMappings(
	ctx context.Context,
	visManager manager.VisibilityManager,
	searchAttributesProvider searchattribute.Provider,
) error {
	searchAttributes, err := searchAttributesProvider.GetSearchAttributes(visManager.GetIndexName(), false)
	if err != nil {
		return err
	}
	preallocatedFields := make(map[string]enumspb.IndexedValueType)
	for name, valueType := range searchAttributes.Custom() {
		if sadefs.IsPreallocatedCSAFieldName(name, valueType) {
			preallocatedFields[name] = valueType
		}
	}
	if len(preallocatedFields) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, registerPreallocatedSearchAttributesAttemptTimeout)
	defer cancel()
	return visManager.AddSearchAttributes(ctx, &manager.AddSearchAttributesRequest{SearchAttributes: preallocatedFields})
}
