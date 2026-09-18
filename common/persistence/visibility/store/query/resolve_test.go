package query

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/searchattribute"
)

// mapAllMapper is a mapper that resolves every alias to the same field name, to assert that
// search attributes that are not mappable are not resolved through the namespace mapper.
type mapAllMapper string

var _ searchattribute.Mapper = mapAllMapper("")

func (m mapAllMapper) GetAlias(fieldName string, _ string) (string, error) {
	return fieldName, nil
}

func (m mapAllMapper) GetFieldName(_ string, _ string) (string, error) {
	return string(m), nil
}

func TestResolveSearchAttributeAlias(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name                 string
		in                   string
		withCustomScheduleID bool
		// saMapper overrides the default searchattribute.TestMapper.
		saMapper    searchattribute.Mapper
		archetypeID chasm.ArchetypeID
		outFn       string
		outFt       enumspb.IndexedValueType
		err         string
	}{
		{
			name:  "success system StartTime",
			in:    "StartTime",
			outFn: "StartTime",
			outFt: enumspb.INDEXED_VALUE_TYPE_DATETIME,
		},

		{
			name:  "success system WorkflowId",
			in:    "WorkflowId",
			outFn: "WorkflowId",
			outFt: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},

		{
			name:  "success reserved BuildIds",
			in:    "BuildIds",
			outFn: "BuildIds",
			outFt: enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
		},

		{
			name:  "success reserved TemporalBuildIds",
			in:    "TemporalBuildIds",
			outFn: "BuildIds",
			outFt: enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
		},

		{
			name:  "success reserved TemporalWorkerDeployment",
			in:    "TemporalWorkerDeployment",
			outFn: "TemporalWorkerDeployment",
			outFt: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},

		{
			name:  "success reserved WorkerDeployment",
			in:    "WorkerDeployment",
			outFn: "TemporalWorkerDeployment",
			outFt: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},

		{
			name:  "success custom AliasForInt01",
			in:    "AliasForInt01",
			outFn: "Int01",
			outFt: enumspb.INDEXED_VALUE_TYPE_INT,
		},

		{
			name:     "success custom noop mapper Int01",
			in:       "Int01",
			saMapper: &searchattribute.NoopMapper{},
			outFn:    "Int01",
			outFt:    enumspb.INDEXED_VALUE_TYPE_INT,
		},

		{
			// System search attributes are not mappable, so the namespace mapper must not
			// be able to resolve them to another field.
			name:     "success system not resolved by mapper",
			in:       "WorkflowId",
			saMapper: mapAllMapper("Int01"),
			outFn:    "WorkflowId",
			outFt:    enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},

		{
			// Predefined search attributes are not mappable either.
			name:     "success predefined not resolved by mapper",
			in:       "TemporalSchedulePaused",
			saMapper: mapAllMapper("Int01"),
			outFn:    "TemporalSchedulePaused",
			outFt:    enumspb.INDEXED_VALUE_TYPE_BOOL,
		},

		{
			name:  "success special ScheduleId",
			in:    "ScheduleId",
			outFn: "WorkflowId",
			outFt: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},

		{
			name:  "success special TemporalScheduleId",
			in:    "TemporalScheduleId",
			outFn: "WorkflowId",
			outFt: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},

		{
			name:                 "success custom ScheduleId",
			in:                   "ScheduleId",
			withCustomScheduleID: true,
			outFn:                searchattribute.TestScheduleIDFieldName,
			outFt:                enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},

		{
			name:                 "success custom ScheduleId reserved TemporalScheduleId",
			in:                   "TemporalScheduleId",
			withCustomScheduleID: true,
			outFn:                "WorkflowId",
			outFt:                enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},

		{
			name:     "success noop mapper ScheduleId",
			in:       "ScheduleId",
			saMapper: &searchattribute.NoopMapper{},
			outFn:    "WorkflowId",
			outFt:    enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},

		{
			name:     "success noop mapper TemporalScheduleId",
			in:       "TemporalScheduleId",
			saMapper: &searchattribute.NoopMapper{},
			outFn:    "WorkflowId",
			outFt:    enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},

		{
			// The ScheduleId alias is resolved before the scheduler archetype aliases.
			name:        "success scheduler archetype ScheduleId",
			in:          "ScheduleId",
			archetypeID: chasm.SchedulerArchetypeID,
			outFn:       "WorkflowId",
			outFt:       enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},

		{
			// TemporalSystemExecutionStatus allows querying Workflow based and CHASM based
			// schedulers with the same alias.
			name:        "success scheduler archetype TemporalSystemExecutionStatus",
			in:          "TemporalSystemExecutionStatus",
			archetypeID: chasm.SchedulerArchetypeID,
			outFn:       "ExecutionStatus",
			outFt:       enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},

		{
			name: "invalid TemporalSystemExecutionStatus without archetype",
			in:   "TemporalSystemExecutionStatus",
			err:  "invalid search attribute: TemporalSystemExecutionStatus",
		},

		{
			name:        "invalid TemporalSystemExecutionStatus with other archetype",
			in:          "TemporalSystemExecutionStatus",
			archetypeID: chasm.SchedulerArchetypeID + 1,
			err:         "invalid search attribute: TemporalSystemExecutionStatus",
		},

		{
			name: "invalid search attribute",
			in:   "Foo",
			err:  "invalid search attribute: Foo",
		},

		{
			// The mapper resolves the alias, but the field is not in the type map.
			name: "invalid custom search attribute unknown field",
			in:   "AliasForFoo",
			err:  "invalid search attribute: AliasForFoo",
		},

		{
			name: "invalid search attribute with reserved prefix",
			in:   "TemporalFoo",
			err:  "invalid search attribute: TemporalFoo",
		},

		{
			name: "invalid empty search attribute",
			in:   "",
			err:  "invalid search attribute: ",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			saMapper := tc.saMapper
			if saMapper == nil {
				saMapper = &searchattribute.TestMapper{
					WithCustomScheduleID: tc.withCustomScheduleID,
				}
			}

			fn, ft, err := ResolveSearchAttributeAlias(
				tc.in,
				testNamespaceName,
				saMapper,
				searchattribute.TestNameTypeMap(),
				nil, // chasmMapper
				tc.archetypeID,
			)
			if tc.err != "" {
				r.Error(err)
				r.ErrorContains(err, tc.err)
				var expectedErr *ConverterError
				r.ErrorAs(err, &expectedErr)
				r.Empty(fn)
				r.Equal(enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, ft)
			} else {
				r.NoError(err)
				r.Equal(tc.outFn, fn)
				r.Equal(tc.outFt, ft)
			}
		})
	}
}

func TestResolveSearchAttributeAlias_WithChasmMapper(t *testing.T) {
	t.Parallel()

	chasmMapper := chasm.NewTestVisibilitySearchAttributesMapper(
		map[string]string{
			"TemporalBool01":    "ChasmCompleted",
			"TemporalKeyword01": "ChasmStatus",
			// Shadows the ExecutionStatus system search attribute.
			"TemporalKeyword02": "ExecutionStatus",
			// Shadows the alias of the Keyword01 custom search attribute.
			"TemporalKeyword03": "AliasForKeyword01",
		},
		map[string]enumspb.IndexedValueType{
			"TemporalBool01":    enumspb.INDEXED_VALUE_TYPE_BOOL,
			"TemporalKeyword01": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			"TemporalKeyword02": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			"TemporalKeyword03": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
	)

	testCases := []struct {
		name                    string
		expectedFieldName       string
		expectedFieldType       enumspb.IndexedValueType
		expectedErr             bool
		expectNamespaceDivision bool
	}{
		{
			name:                    "ChasmCompleted",
			expectedFieldName:       "TemporalBool01",
			expectedFieldType:       enumspb.INDEXED_VALUE_TYPE_BOOL,
			expectedErr:             false,
			expectNamespaceDivision: false,
		},
		{
			name:                    "ChasmStatus",
			expectedFieldName:       "TemporalKeyword01",
			expectedFieldType:       enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			expectedErr:             false,
			expectNamespaceDivision: false,
		},
		{
			name:                    "TemporalNamespaceDivision",
			expectedFieldName:       "TemporalNamespaceDivision",
			expectedFieldType:       enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			expectedErr:             false,
			expectNamespaceDivision: true,
		},
		{
			// A CHASM search attribute takes precedence over a system search attribute,
			// which is not mappable.
			name:                    "ExecutionStatus",
			expectedFieldName:       "TemporalKeyword02",
			expectedFieldType:       enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			expectedErr:             false,
			expectNamespaceDivision: false,
		},
		{
			// A custom search attribute takes precedence over a CHASM search attribute.
			name:                    "AliasForKeyword01",
			expectedFieldName:       "Keyword01",
			expectedFieldType:       enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			expectedErr:             false,
			expectNamespaceDivision: false,
		},
		{
			name:                    "NonExistentChasmAlias",
			expectedFieldName:       "",
			expectedFieldType:       enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED,
			expectedErr:             true,
			expectNamespaceDivision: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			fieldName, fieldType, err := ResolveSearchAttributeAlias(
				tc.name,
				testNamespaceName,
				&searchattribute.TestMapper{},
				searchattribute.TestNameTypeMap(),
				chasmMapper,
				chasm.UnspecifiedArchetypeID,
			)
			if tc.expectedErr {
				r.Error(err)
				r.Empty(fieldName)
				r.Equal(enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, fieldType)
			} else {
				r.NoError(err)
				r.Equal(tc.expectedFieldName, fieldName)
				r.Equal(tc.expectedFieldType, fieldType)
			}
		})
	}
}
