package frontend

import (
	"context"
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/searchattribute/sadefs"
)

const backfillListNamespacesPageSize = 1000

// InitializeSearchAttributeMappings backfills namespace custom search attribute
// aliases from cluster metadata with identity mappings. Runs on frontend startup
// when Elasticsearch visibility is configured.
func InitializeSearchAttributeMappings(
	ctx context.Context,
	metadataManager persistence.MetadataManager,
	clusterMetadataManager persistence.ClusterMetadataManager,
	currentClusterName string,
	visibilityIndexName string,
	handler Handler,
	logger log.Logger,
) error {
	clusterMetadataResponse, err := clusterMetadataManager.GetClusterMetadata(
		ctx,
		&persistence.GetClusterMetadataRequest{ClusterName: currentClusterName},
	)
	if err != nil {
		return fmt.Errorf("failed to fetch cluster metadata: %w", err)
	}

	indexSearchAttrs := clusterMetadataResponse.IndexSearchAttributes[visibilityIndexName]
	if indexSearchAttrs == nil || len(indexSearchAttrs.CustomSearchAttributes) == 0 {
		return nil
	}

	request := &persistence.ListNamespacesRequest{
		PageSize:       backfillListNamespacesPageSize,
		IncludeDeleted: false,
	}

	for {
		response, err := metadataManager.ListNamespaces(ctx, request)
		if err != nil {
			return fmt.Errorf("failed to list namespaces: %w", err)
		}

		for _, nsDetail := range response.Namespaces {
			if err := updateSingleNamespaceSearchAttributeMappings(
				ctx,
				handler,
				nsDetail,
				indexSearchAttrs.CustomSearchAttributes,
				logger,
			); err != nil {
				return fmt.Errorf("failed to update namespace search attribute mappings: %w", err)
			}
		}

		if len(response.NextPageToken) == 0 {
			break
		}
		request.NextPageToken = response.NextPageToken
	}

	return nil
}

func updateSingleNamespaceSearchAttributeMappings(
	ctx context.Context,
	handler Handler,
	nsDetail *persistence.GetNamespaceResponse,
	clusterCustomSearchAttributes map[string]enumspb.IndexedValueType,
	logger log.Logger,
) error {
	fieldToAliasMap := nsDetail.Namespace.Config.CustomSearchAttributeAliases
	if fieldToAliasMap == nil {
		fieldToAliasMap = make(map[string]string)
	}

	upsertFieldToAliasMap := make(map[string]string)
	for fieldName, fieldType := range clusterCustomSearchAttributes {
		if sadefs.IsPreallocatedCSAFieldName(fieldName, fieldType) {
			continue
		}
		if _, ok := fieldToAliasMap[fieldName]; ok {
			continue
		}
		upsertFieldToAliasMap[fieldName] = fieldName
	}

	if len(upsertFieldToAliasMap) == 0 {
		return nil
	}

	nsName := nsDetail.Namespace.Info.Name
	_, err := handler.UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace: nsName,
		Config: &namespacepb.NamespaceConfig{
			CustomSearchAttributeAliases: upsertFieldToAliasMap,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to update namespace search attribute mappings: %w", err)
	}
	logger.Info("Created identity mappings for namespace search attributes",
		tag.WorkflowNamespace(nsName),
		tag.Number(int64(len(upsertFieldToAliasMap))))
	return nil
}
