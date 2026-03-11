package query

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/searchattribute/sadefs"
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
		{name: sadefs.ScheduleID, expectedFieldName: sadefs.WorkflowID, expectedFieldType: enumspb.INDEXED_VALUE_TYPE_KEYWORD, expectedErr: false},
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
			fieldName, fieldType, err := ResolveSearchAttributeAlias(tc.name, ns, mapper, saTypeMap, nil)
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
		sadefs.ScheduleID: enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
	})

	mapper := customMapper{
		fieldToAlias: map[string]string{
			sadefs.ScheduleID: sadefs.ScheduleID,
		},
		aliasToField: map[string]string{
			sadefs.ScheduleID: sadefs.ScheduleID,
		},
	}

	// When ScheduleID is a custom search attribute, it should use the custom attribute, not transform to WorkflowID
	fieldName, fieldType, err := ResolveSearchAttributeAlias(sadefs.ScheduleID, ns, mapper, saTypeMapWithCustomScheduleID, nil)
	require.NoError(t, err)
	require.Equal(t, sadefs.ScheduleID, fieldName)
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

func TestResolveSearchAttributeAlias_WithChasmMapper(t *testing.T) {
	ns := namespace.Name("test-namespace")
	saTypeMap := searchattribute.NewNameTypeMapStub(map[string]enumspb.IndexedValueType{
		"ExecutionStatus": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"StartTime":       enumspb.INDEXED_VALUE_TYPE_DATETIME,
	})
	mapper := customMapper{
		fieldToAlias: map[string]string{},
		aliasToField: map[string]string{},
	}

	chasmMapper := chasm.NewTestVisibilitySearchAttributesMapper(
		map[string]string{
			"TemporalBool01":    "ChasmCompleted",
			"TemporalKeyword01": "ChasmStatus",
		},
		map[string]enumspb.IndexedValueType{
			"TemporalBool01":    enumspb.INDEXED_VALUE_TYPE_BOOL,
			"TemporalKeyword01": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
	)

	testCases := []struct {
		name              string
		chasmMapper       *chasm.VisibilitySearchAttributesMapper
		expectedFieldName string
		expectedFieldType enumspb.IndexedValueType
		expectedErr       bool
	}{
		{
			name:              "ChasmCompleted",
			chasmMapper:       chasmMapper,
			expectedFieldName: "TemporalBool01",
			expectedFieldType: enumspb.INDEXED_VALUE_TYPE_BOOL,
			expectedErr:       false,
		},
		{
			name:              "ChasmStatus",
			chasmMapper:       chasmMapper,
			expectedFieldName: "TemporalKeyword01",
			expectedFieldType: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			expectedErr:       false,
		},
		{
			name:              "ExecutionStatus",
			chasmMapper:       chasmMapper,
			expectedFieldName: "ExecutionStatus",
			expectedFieldType: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			expectedErr:       false,
		},
		{
			name:              "NonExistentChasmAlias",
			chasmMapper:       chasmMapper,
			expectedFieldName: "",
			expectedFieldType: enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED,
			expectedErr:       true,
		},
		{
			name:              "ChasmCompleted",
			chasmMapper:       nil,
			expectedFieldName: "",
			expectedFieldType: enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED,
			expectedErr:       true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fieldName, fieldType, err := ResolveSearchAttributeAlias(tc.name, ns, mapper, saTypeMap, tc.chasmMapper)
			require.Equal(t, tc.expectedFieldName, fieldName)
			require.Equal(t, tc.expectedFieldType, fieldType)
			require.Equal(t, tc.expectedErr, err != nil)
		})
	}
}

func TestResolveSearchAttributeAlias_ChasmOverridesSystemAttribute(t *testing.T) {
	ns := namespace.Name("test-namespace")
	saTypeMap := searchattribute.NewNameTypeMapStub(map[string]enumspb.IndexedValueType{
		"ExecutionStatus": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	})
	mapper := customMapper{
		fieldToAlias: map[string]string{},
		aliasToField: map[string]string{},
	}

	chasmMapper := chasm.NewTestVisibilitySearchAttributesMapper(
		map[string]string{
			"TemporalLowCardinalityKeyword01": "ExecutionStatus",
		},
		map[string]enumspb.IndexedValueType{
			"TemporalLowCardinalityKeyword01": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
	)

	fieldName, fieldType, err := ResolveSearchAttributeAlias("ExecutionStatus", ns, mapper, saTypeMap, chasmMapper)
	require.NoError(t, err)
	require.Equal(t, "TemporalLowCardinalityKeyword01", fieldName)
	require.Equal(t, enumspb.INDEXED_VALUE_TYPE_KEYWORD, fieldType)
}

func TestResolveSearchAttributeAlias_ChasmPriority(t *testing.T) {
	ns := namespace.Name("test-namespace")
	saTypeMap := searchattribute.NewNameTypeMapStub(map[string]enumspb.IndexedValueType{
		"MyCustomField": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"StartTime":     enumspb.INDEXED_VALUE_TYPE_DATETIME,
	})
	mapper := customMapper{
		fieldToAlias: map[string]string{
			"MyCustomField": "MyCustomField",
		},
		aliasToField: map[string]string{
			"MyCustomField": "MyCustomField",
		},
	}

	chasmMapper := chasm.NewTestVisibilitySearchAttributesMapper(
		map[string]string{
			"TemporalKeyword01": "MyCustomField",
		},
		map[string]enumspb.IndexedValueType{
			"TemporalKeyword01": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
	)

	// Custom SA should take precedence over CHASM SA
	fieldName, fieldType, err := ResolveSearchAttributeAlias("MyCustomField", ns, mapper, saTypeMap, chasmMapper)
	require.NoError(t, err)
	require.Equal(t, "MyCustomField", fieldName)
	require.Equal(t, enumspb.INDEXED_VALUE_TYPE_KEYWORD, fieldType)

	// CHASM SA should be used when custom SA doesn't exist
	chasmMapper2 := chasm.NewTestVisibilitySearchAttributesMapper(
		map[string]string{
			"TemporalKeyword01": "ChasmOnlyField",
		},
		map[string]enumspb.IndexedValueType{
			"TemporalKeyword01": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
	)
	fieldName2, fieldType2, err2 := ResolveSearchAttributeAlias("ChasmOnlyField", ns, mapper, saTypeMap, chasmMapper2)
	require.NoError(t, err2)
	require.Equal(t, "TemporalKeyword01", fieldName2)
	require.Equal(t, enumspb.INDEXED_VALUE_TYPE_KEYWORD, fieldType2)
}

func TestResolveSearchAttributeAlias_ChasmMissingType(t *testing.T) {
	ns := namespace.Name("test-namespace")
	saTypeMap := searchattribute.NewNameTypeMapStub(map[string]enumspb.IndexedValueType{
		"StartTime": enumspb.INDEXED_VALUE_TYPE_DATETIME,
	})
	mapper := customMapper{
		fieldToAlias: map[string]string{},
		aliasToField: map[string]string{},
	}

	// CHASM mapper with field that doesn't exist in type map
	chasmMapper := chasm.NewTestVisibilitySearchAttributesMapper(
		map[string]string{
			"TemporalKeyword01": "ChasmField",
		},
		map[string]enumspb.IndexedValueType{
			// Missing TemporalKeyword01 in type map
		},
	)

	// Should fall back to system SA resolution
	fieldName, fieldType, err := ResolveSearchAttributeAlias("ChasmField", ns, mapper, saTypeMap, chasmMapper)
	require.Error(t, err)
	require.Empty(t, fieldName)
	require.Equal(t, enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, fieldType)
}
