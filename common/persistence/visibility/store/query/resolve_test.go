package query

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/searchattribute"
)

func TestResolveSearchAttributeAlias(t *testing.T) {
	cases := []struct {
		name              string
		expectedFieldName string
		expectedFieldType enumspb.IndexedValueType
		expectedErr       bool
	}{
		{name: "MyCustomField", expectedFieldName: "MyCustomField", expectedFieldType: enumspb.INDEXED_VALUE_TYPE_KEYWORD, expectedErr: false},
		{name: "ExecutionStatus", expectedFieldName: "ExecutionStatus", expectedFieldType: enumspb.INDEXED_VALUE_TYPE_KEYWORD, expectedErr: false},
		{name: "ScheduledStartTime", expectedFieldName: "TemporalScheduledStartTime", expectedFieldType: enumspb.INDEXED_VALUE_TYPE_DATETIME, expectedErr: false},
		{name: "SchedulePaused", expectedFieldName: "TemporalSchedulePaused", expectedFieldType: enumspb.INDEXED_VALUE_TYPE_BOOL, expectedErr: false},
		{name: "BuildIds", expectedFieldName: "BuildIds", expectedFieldType: enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, expectedErr: false},
		{name: "TemporalBuildIds", expectedFieldName: "BuildIds", expectedFieldType: enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, expectedErr: false},
		{name: "TemporalPauseInfo", expectedFieldName: "TemporalPauseInfo", expectedFieldType: enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, expectedErr: false},
		{name: "NonExistentField", expectedFieldName: "", expectedFieldType: enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, expectedErr: true},
		{name: searchattribute.ScheduleID, expectedFieldName: searchattribute.WorkflowID, expectedFieldType: enumspb.INDEXED_VALUE_TYPE_KEYWORD, expectedErr: false},
	}

	ns := namespace.Name("test-namespace")
	saTypeMap := searchattribute.NewNameTypeMapStub(map[string]enumspb.IndexedValueType{
		"MyCustomField":   enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"ExecutionStatus": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	})
	mapper := customMapper{
		fieldToAlias: map[string]string{
			"MyCustomField":   "some-alias",
			"ExecutionStatus": "some-alias2",
		},
		aliasToField: map[string]string{
			"some-alias":  "MyCustomField",
			"some-alias2": "ExecutionStatus",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fieldName, fieldType, err := ResolveSearchAttributeAlias(tc.name, ns, mapper, saTypeMap)
			require.Equal(t, tc.expectedFieldName, fieldName)
			require.Equal(t, tc.expectedFieldType, fieldType)
			require.Equal(t, tc.expectedErr, err != nil)
		})
	}
}

func TestResolveSearchAttributeAlias_CustomScheduleID(t *testing.T) {
	ns := namespace.Name("test-namespace")

	// Test case where ScheduleID is defined as a custom search attribute
	saTypeMapWithCustomScheduleID := searchattribute.NewNameTypeMapStub(map[string]enumspb.IndexedValueType{
		searchattribute.ScheduleID: enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
	})

	mapper := customMapper{
		fieldToAlias: map[string]string{
			searchattribute.ScheduleID: searchattribute.ScheduleID,
		},
		aliasToField: map[string]string{
			searchattribute.ScheduleID: searchattribute.ScheduleID,
		},
	}

	// When ScheduleID is a custom search attribute, it should use the custom attribute, not transform to WorkflowID
	fieldName, fieldType, err := ResolveSearchAttributeAlias(searchattribute.ScheduleID, ns, mapper, saTypeMapWithCustomScheduleID)
	require.NoError(t, err)
	require.Equal(t, searchattribute.ScheduleID, fieldName)
	require.Equal(t, enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, fieldType)
}

type customMapper struct {
	fieldToAlias map[string]string
	aliasToField map[string]string
}

func (m customMapper) GetAlias(fieldName string, ns string) (string, error) {
	if fn, ok := m.fieldToAlias[fieldName]; ok {
		return fn, nil
	}
	return "", serviceerror.NewInvalidArgument(
		fmt.Sprintf("Namespace %s has no mapping defined for field name %s", ns, fieldName),
	)
}

func (m customMapper) GetFieldName(alias string, ns string) (string, error) {
	if fn, ok := m.aliasToField[alias]; ok {
		return fn, nil
	}
	return "", serviceerror.NewInvalidArgument(
		fmt.Sprintf("Namespace %s has no mapping defined for search attribute %s", ns, alias),
	)
}
