package frontend

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
	"go.temporal.io/server/common/persistence/visibility"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/esconfig"
	"go.temporal.io/server/common/searchattribute"
	"go.uber.org/mock/gomock"
)

func TestValidateSearchAttributeAliases(t *testing.T) {
	esSearchAttributes := searchattribute.NewNameTypeMap(map[string]enumspb.IndexedValueType{
		"CustomerId": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"Keyword01":  enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	})

	newElasticsearchHandler := func(ctrl *gomock.Controller) *namespaceHandler {
		visibilityMgr := manager.NewMockVisibilityManager(ctrl)
		visibilityMgr.EXPECT().HasStoreName(esconfig.PersistenceName).Return(true)
		visibilityMgr.EXPECT().GetIndexName().Return(testIndexName)
		saProvider := searchattribute.NewMockProvider(ctrl)
		saProvider.EXPECT().GetSearchAttributes(testIndexName, false).Return(esSearchAttributes, nil)
		return &namespaceHandler{visibilityMgr: visibilityMgr, saProvider: saProvider}
	}

	for _, tc := range []struct {
		name          string
		aliases       map[string]string
		expectedError string
	}{
		{
			name:    "new alias",
			aliases: map[string]string{"Keyword02": "NewAttr"},
		},
		{
			name:    "alias removal",
			aliases: map[string]string{"Keyword02": ""},
		},
		{
			name:          "pre-allocated field name",
			aliases:       map[string]string{"Keyword02": "Text01"},
			expectedError: "Search attribute Text01 is a pre-allocated custom search attribute field name",
		},
		{
			name:          "cluster-level custom search attribute",
			aliases:       map[string]string{"Keyword02": "CustomerId"},
			expectedError: "Search attribute CustomerId is already a cluster-level custom search attribute",
		},
	} {
		t.Run("elasticsearch/"+tc.name, func(t *testing.T) {
			err := newElasticsearchHandler(gomock.NewController(t)).validateSearchAttributeAliases(tc.aliases)
			if tc.expectedError == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.expectedError)
			_, ok := errors.AsType[*serviceerror.InvalidArgument](err)
			require.True(t, ok)
		})
	}

	t.Run("sql allows pre-allocated field names", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		visibilityMgr := manager.NewMockVisibilityManager(ctrl)
		visibilityMgr.EXPECT().HasStoreName(esconfig.PersistenceName).Return(false)
		handler := &namespaceHandler{visibilityMgr: visibilityMgr, saProvider: searchattribute.NewMockProvider(ctrl)}
		require.NoError(t, handler.validateSearchAttributeAliases(map[string]string{"Keyword02": "Keyword01"}))
	})

	t.Run("dual visibility is not validated", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		sqlManager := manager.NewMockVisibilityManager(ctrl)
		sqlManager.EXPECT().GetStoreNames().Return([]string{mysql.PluginName}).AnyTimes()
		esManager := manager.NewMockVisibilityManager(ctrl)
		esManager.EXPECT().GetStoreNames().Return([]string{esconfig.PersistenceName}).AnyTimes()
		handler := &namespaceHandler{
			visibilityMgr: visibility.NewVisibilityManagerDual(sqlManager, esManager, nil, dynamicconfig.GetBoolPropertyFn(false)),
			saProvider:    searchattribute.NewMockProvider(ctrl),
		}
		require.NoError(t, handler.validateSearchAttributeAliases(map[string]string{"Keyword02": "CustomerId"}))
	})
}
