package history

import (
	"context"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence/visibility"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/esconfig"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/searchattribute/sadefs"
)

const registerPreallocatedSearchAttributesTimeout = time.Minute

// RegisterPreallocatedSearchAttributeMappings adds the pre-allocated custom search attribute
// fields (eg: Keyword01) listed in cluster metadata to the mappings of the Elasticsearch
// visibility indexes at startup. The index mapping is not dynamic, so values of unmapped fields
// are not indexed, and queries on their namespace aliases return nothing.
// Failures are logged and don't block startup.
func RegisterPreallocatedSearchAttributeMappings(
	visibilityManager manager.VisibilityManager,
	searchAttributesProvider searchattribute.Provider,
	logger log.Logger,
) {
	visibilityManagers := []manager.VisibilityManager{visibilityManager}
	if visibilityManagerDual, ok := visibilityManager.(*visibility.VisibilityManagerDual); ok {
		visibilityManagers = []manager.VisibilityManager{
			visibilityManagerDual.GetPrimaryVisibility(),
			visibilityManagerDual.GetSecondaryVisibility(),
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), registerPreallocatedSearchAttributesTimeout)
	defer cancel()
	for _, visManager := range visibilityManagers {
		// Other visibility stores don't support adding search attributes.
		if visManager.GetStoreNames()[0] != esconfig.PersistenceName {
			continue
		}
		indexName := visManager.GetIndexName()
		searchAttributes, err := searchAttributesProvider.GetSearchAttributes(indexName, false)
		if err != nil {
			logger.Error("Failed to register pre-allocated custom search attribute mappings.",
				tag.ESIndex(indexName), tag.Error(err))
			continue
		}
		preallocatedFields := make(map[string]enumspb.IndexedValueType)
		for name, valueType := range searchAttributes.Custom() {
			if sadefs.IsPreallocatedCSAFieldName(name, valueType) {
				preallocatedFields[name] = valueType
			}
		}
		if len(preallocatedFields) == 0 {
			continue
		}
		err = visManager.AddSearchAttributes(ctx, &manager.AddSearchAttributesRequest{SearchAttributes: preallocatedFields})
		if err != nil {
			logger.Error("Failed to register pre-allocated custom search attribute mappings.",
				tag.ESIndex(indexName), tag.Error(err))
			continue
		}
		logger.Info("Registered pre-allocated custom search attribute mappings.", tag.ESIndex(indexName))
	}
}
