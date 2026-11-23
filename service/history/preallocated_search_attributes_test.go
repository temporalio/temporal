package history

import (
	"errors"
	"maps"
	"testing"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
	"go.temporal.io/server/common/persistence/visibility"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/esconfig"
	"go.temporal.io/server/common/searchattribute"
	"go.uber.org/mock/gomock"
)

const preallocatedFieldsTestIndex = "test-index"

// esClusterMetadataTypeMap returns the custom search attributes of an Elasticsearch cluster with
// legacy cluster-level search attributes and the pre-allocated fields in cluster metadata.
func esClusterMetadataTypeMap() searchattribute.NameTypeMap {
	customSearchAttributes := maps.Clone(searchattribute.TestEsNameTypeMap().Custom())
	maps.Copy(customSearchAttributes, searchattribute.TestNameTypeMap().Custom())
	return searchattribute.NewNameTypeMap(customSearchAttributes)
}

func newMockVisibilityManager(ctrl *gomock.Controller, storeName string) *manager.MockVisibilityManager {
	visManager := manager.NewMockVisibilityManager(ctrl)
	visManager.EXPECT().GetStoreNames().Return([]string{storeName}).AnyTimes()
	visManager.EXPECT().GetIndexName().Return(preallocatedFieldsTestIndex).AnyTimes()
	return visManager
}

func TestRegisterPreallocatedSearchAttributeMappings(t *testing.T) {
	expectedRequest := &manager.AddSearchAttributesRequest{
		SearchAttributes: searchattribute.TestNameTypeMap().Custom(),
	}

	testCases := []struct {
		name  string
		setup func(ctrl *gomock.Controller, saProvider *searchattribute.MockProvider) manager.VisibilityManager
	}{
		{
			name: "elasticsearch registers only the pre-allocated fields",
			setup: func(ctrl *gomock.Controller, saProvider *searchattribute.MockProvider) manager.VisibilityManager {
				visManager := newMockVisibilityManager(ctrl, esconfig.PersistenceName)
				saProvider.EXPECT().GetSearchAttributes(preallocatedFieldsTestIndex, false).Return(esClusterMetadataTypeMap(), nil)
				visManager.EXPECT().AddSearchAttributes(gomock.Any(), expectedRequest).Return(nil)
				return visManager
			},
		},
		{
			name: "elasticsearch failure doesn't block startup",
			setup: func(ctrl *gomock.Controller, saProvider *searchattribute.MockProvider) manager.VisibilityManager {
				visManager := newMockVisibilityManager(ctrl, esconfig.PersistenceName)
				saProvider.EXPECT().GetSearchAttributes(preallocatedFieldsTestIndex, false).Return(esClusterMetadataTypeMap(), nil)
				visManager.EXPECT().AddSearchAttributes(gomock.Any(), expectedRequest).Return(errors.New("forbidden"))
				return visManager
			},
		},
		{
			name: "cluster metadata lookup failure skips registration",
			setup: func(ctrl *gomock.Controller, saProvider *searchattribute.MockProvider) manager.VisibilityManager {
				visManager := newMockVisibilityManager(ctrl, esconfig.PersistenceName)
				saProvider.EXPECT().GetSearchAttributes(preallocatedFieldsTestIndex, false).
					Return(searchattribute.NameTypeMap{}, errors.New("unavailable"))
				return visManager
			},
		},
		{
			name: "no pre-allocated fields in cluster metadata",
			setup: func(ctrl *gomock.Controller, saProvider *searchattribute.MockProvider) manager.VisibilityManager {
				visManager := newMockVisibilityManager(ctrl, esconfig.PersistenceName)
				saProvider.EXPECT().GetSearchAttributes(preallocatedFieldsTestIndex, false).Return(searchattribute.TestEsNameTypeMap(), nil)
				return visManager
			},
		},
		{
			name: "sql is skipped",
			setup: func(ctrl *gomock.Controller, _ *searchattribute.MockProvider) manager.VisibilityManager {
				return newMockVisibilityManager(ctrl, mysql.PluginName)
			},
		},
		{
			name: "dual visibility registers only the elasticsearch store",
			setup: func(ctrl *gomock.Controller, saProvider *searchattribute.MockProvider) manager.VisibilityManager {
				sqlManager := newMockVisibilityManager(ctrl, mysql.PluginName)
				esManager := newMockVisibilityManager(ctrl, esconfig.PersistenceName)
				saProvider.EXPECT().GetSearchAttributes(preallocatedFieldsTestIndex, false).Return(esClusterMetadataTypeMap(), nil)
				esManager.EXPECT().AddSearchAttributes(gomock.Any(), expectedRequest).Return(nil)
				return visibility.NewVisibilityManagerDual(sqlManager, esManager, nil, nil)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			saProvider := searchattribute.NewMockProvider(ctrl)
			visManager := tc.setup(ctrl, saProvider)
			// Unexpected calls on the mocks fail the test.
			RegisterPreallocatedSearchAttributeMappings(visManager, saProvider, log.NewNoopLogger())
		})
	}
}
