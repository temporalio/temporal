package frontend

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.uber.org/mock/gomock"
)

type testBackfillHandler struct {
	workflowservice.UnimplementedWorkflowServiceServer
	updateRequests []*workflowservice.UpdateNamespaceRequest
	updateErr      error
}

func (h *testBackfillHandler) GetConfig() *Config {
	return &Config{}
}

func (h *testBackfillHandler) Start() {}

func (h *testBackfillHandler) Stop() {}

func (h *testBackfillHandler) UpdateNamespace(
	_ context.Context,
	request *workflowservice.UpdateNamespaceRequest,
) (*workflowservice.UpdateNamespaceResponse, error) {
	h.updateRequests = append(h.updateRequests, request)
	if h.updateErr != nil {
		return nil, h.updateErr
	}
	return &workflowservice.UpdateNamespaceResponse{}, nil
}

func TestBuildIdentitySearchAttributeAliases(t *testing.T) {
	t.Parallel()

	aliases := buildIdentitySearchAttributeAliases(
		map[string]string{
			"legacy_text": "legacy_text",
		},
		map[string]enumspb.IndexedValueType{
			"Keyword01":      enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			"legacy_keyword": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			"legacy_text":    enumspb.INDEXED_VALUE_TYPE_TEXT,
		},
	)

	require.Equal(t, map[string]string{
		"legacy_keyword": "legacy_keyword",
	}, aliases)
}

func TestInitializeSearchAttributeMappings_BackfillsLegacyClusterMetadataFields(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	defer controller.Finish()

	metadataManager := persistence.NewMockMetadataManager(controller)
	clusterMetadataManager := persistence.NewMockClusterMetadataManager(controller)
	handler := &testBackfillHandler{}

	clusterMetadataManager.EXPECT().GetClusterMetadata(
		gomock.Any(),
		&persistence.GetClusterMetadataRequest{ClusterName: "active"},
	).Return(&persistence.GetClusterMetadataResponse{
		ClusterMetadata: &persistencespb.ClusterMetadata{
			ClusterName: "active",
			IndexSearchAttributes: map[string]*persistencespb.IndexSearchAttributes{
				"visibility-index": {
					CustomSearchAttributes: map[string]enumspb.IndexedValueType{
						"Keyword01":      enumspb.INDEXED_VALUE_TYPE_KEYWORD,
						"legacy_keyword": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
					},
				},
			},
		},
	}, nil)

	metadataManager.EXPECT().ListNamespaces(
		gomock.Any(),
		&persistence.ListNamespacesRequest{
			PageSize:       backfillListNamespacesPageSize,
			IncludeDeleted: false,
		},
	).Return(&persistence.ListNamespacesResponse{
		Namespaces: []*persistence.GetNamespaceResponse{
			{
				Namespace: &persistencespb.NamespaceDetail{
					Info: &persistencespb.NamespaceInfo{Name: "needs-backfill"},
					Config: &persistencespb.NamespaceConfig{
						CustomSearchAttributeAliases: map[string]string{},
					},
				},
			},
			{
				Namespace: &persistencespb.NamespaceDetail{
					Info: &persistencespb.NamespaceInfo{Name: "already-backfilled"},
					Config: &persistencespb.NamespaceConfig{
						CustomSearchAttributeAliases: map[string]string{
							"legacy_keyword": "legacy_keyword",
						},
					},
				},
			},
		},
	}, nil)

	err := initializeSearchAttributeMappings(
		context.Background(),
		metadataManager,
		clusterMetadataManager,
		"active",
		"visibility-index",
		handler,
		log.NewNoopLogger(),
	)
	require.NoError(t, err)
	require.Len(t, handler.updateRequests, 1)
	require.Equal(t, "needs-backfill", handler.updateRequests[0].Namespace)
	require.Equal(t, &namespacepb.NamespaceConfig{
		CustomSearchAttributeAliases: map[string]string{
			"legacy_keyword": "legacy_keyword",
		},
	}, handler.updateRequests[0].Config)
}

func TestUpdateSingleNamespaceSearchAttributeMappings_IgnoresConditionFailedConflict(t *testing.T) {
	t.Parallel()

	handler := &testBackfillHandler{
		updateErr: &persistence.ConditionFailedError{Msg: "namespace detail changed concurrently"},
	}

	err := updateSingleNamespaceSearchAttributeMappings(
		context.Background(),
		handler,
		&persistence.GetNamespaceResponse{
			Namespace: &persistencespb.NamespaceDetail{
				Info: &persistencespb.NamespaceInfo{Name: "needs-backfill"},
				Config: &persistencespb.NamespaceConfig{
					CustomSearchAttributeAliases: map[string]string{},
				},
			},
		},
		map[string]enumspb.IndexedValueType{
			"legacy_keyword": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
		log.NewNoopLogger(),
	)

	require.NoError(t, err)
	require.Len(t, handler.updateRequests, 1)
}

func TestUpdateSingleNamespaceSearchAttributeMappings_IgnoresConditionalUpdateConflict(t *testing.T) {
	t.Parallel()

	handler := &testBackfillHandler{
		updateErr: &persistence.ConditionFailedError{Msg: "conditional update error: expect: 1, actual: 2"},
	}

	err := updateSingleNamespaceSearchAttributeMappings(
		context.Background(),
		handler,
		&persistence.GetNamespaceResponse{
			Namespace: &persistencespb.NamespaceDetail{
				Info: &persistencespb.NamespaceInfo{Name: "needs-backfill"},
				Config: &persistencespb.NamespaceConfig{
					CustomSearchAttributeAliases: map[string]string{},
				},
			},
		},
		map[string]enumspb.IndexedValueType{
			"legacy_keyword": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
		log.NewNoopLogger(),
	)

	require.NoError(t, err)
	require.Len(t, handler.updateRequests, 1)
}
