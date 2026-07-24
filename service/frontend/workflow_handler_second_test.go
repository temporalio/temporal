package frontend

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	dc "go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/searchattribute"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/metadata"
)

func TestCHASMSchedulerRoutingAndCreationGates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		enableCreation    bool
		enableRouting     bool
		allowedExp        []string
		requestExperiment bool
		expectCreation    bool
		expectRouting     bool
	}{
		{
			name:           "routing-only config routes but does not create",
			enableCreation: false,
			enableRouting:  true,
			expectCreation: false,
			expectRouting:  true,
		},
		{
			name:           "creation config still enables both",
			enableCreation: true,
			enableRouting:  false,
			expectCreation: true,
			expectRouting:  true,
		},
		{
			name:              "experiment enables both",
			enableCreation:    false,
			enableRouting:     false,
			allowedExp:        []string{ChasmSchedulerExperiment},
			requestExperiment: true,
			expectCreation:    true,
			expectRouting:     true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			config := NewConfig(dc.NewNoopCollection(), 1)
			config.EnableCHASMSchedulerCreation = dc.GetBoolPropertyFnFilteredByNamespace(tc.enableCreation)
			config.CHASMSchedulerCreationRolloutPercent = dc.GetIntPropertyFnFilteredByNamespace(100)
			config.EnableCHASMSchedulerRouting = dc.GetBoolPropertyFnFilteredByNamespace(tc.enableRouting)
			config.AllowedExperiments = dc.GetTypedPropertyFnFilteredByNamespace(tc.allowedExp)

			wh := &WorkflowHandler{config: config}

			ctx := context.Background()
			if tc.requestExperiment {
				ctx = metadata.NewIncomingContext(ctx, metadata.Pairs(headers.ExperimentHeaderName, ChasmSchedulerExperiment))
			}

			require.Equal(t, tc.expectCreation, wh.chasmSchedulerCreationEnabled(ctx, "test-namespace", "test-schedule"))
			require.Equal(t, tc.expectRouting, wh.chasmSchedulerEnabled(ctx, "test-namespace"))
		})
	}
}

func TestDescribeScheduleAnnotatesScheduledWorkflowWithTypes(t *testing.T) {
	makeWorkflowHandler := func(
		visibilityManager *manager.MockVisibilityManager,
		searchAttrProvider *searchattribute.MockProvider,
	) WorkflowHandler {
		h := WorkflowHandler{
			saMapperProvider: searchattribute.NewTestMapperProvider(&searchattribute.TestMapper{Namespace: "ns"}),
			visibilityMgr:    visibilityManager,
			saProvider:       searchAttrProvider,
		}
		return h
	}
	controller := gomock.NewController(t)
	visibilityManager := makeVisibilityManagerStub(controller)
	searchAttrProvider := makeSearchAttributesProviderStub(
		controller,
		map[string]enumspb.IndexedValueType{
			"CustomKeywordField": enumspb.INDEXED_VALUE_TYPE_TEXT,
			"CustomBoolField":    enumspb.INDEXED_VALUE_TYPE_BOOL,
		},
	)
	response := makeResponseWithScheduledWorkflowAttributes(
		map[string]string{"CustomKeywordField": "keyword", "CustomBoolField": "true"})

	h := makeWorkflowHandler(visibilityManager, searchAttrProvider)

	err := h.annotateSearchAttributesOfScheduledWorkflow(response, "ns")

	if err != nil {
		t.Fatalf("error %v", err)
	}
	assertScheduledWorkflowSearchAttributeHasAssociatedTypeOf(
		t, response, "CustomKeywordField", enumspb.INDEXED_VALUE_TYPE_TEXT)
	assertScheduledWorkflowSearchAttributeHasAssociatedTypeOf(
		t, response, "CustomBoolField", enumspb.INDEXED_VALUE_TYPE_BOOL)
}

func assertScheduledWorkflowSearchAttributeHasAssociatedTypeOf(
	t *testing.T,
	response *schedulespb.DescribeResponse,
	searchAttribute string,
	expectedType enumspb.IndexedValueType,
) {
	t.Helper()
	attributes := getScheduledWorkflowSearchAttributes(response)
	actualAttributeType := attributes.IndexedFields["AliasFor"+searchAttribute].Metadata[searchattribute.MetadataType]
	if string(actualAttributeType) != expectedType.String() {
		t.Errorf(
			"expected type %s for attribute %s, got %s",
			expectedType.String(),
			searchAttribute,
			string(actualAttributeType),
		)
	}
}

func getScheduledWorkflowSearchAttributes(response *schedulespb.DescribeResponse) *commonpb.SearchAttributes {
	return response.Schedule.Action.Action.(*schedulepb.ScheduleAction_StartWorkflow).StartWorkflow.SearchAttributes
}

func makeSearchAttributesProviderStub(
	controller *gomock.Controller,
	custom map[string]enumspb.IndexedValueType,
) *searchattribute.MockProvider {
	searchAttrProvider := searchattribute.NewMockProvider(controller)
	nameTypeMapStub := searchattribute.NewNameTypeMapStub(custom)
	searchAttrProvider.EXPECT().GetSearchAttributes("index", false).Return(nameTypeMapStub, nil).AnyTimes()
	return searchAttrProvider
}

func makeVisibilityManagerStub(controller *gomock.Controller) *manager.MockVisibilityManager {
	visibilityManager := manager.NewMockVisibilityManager(controller)
	visibilityManager.EXPECT().GetIndexName().Return("index").AnyTimes()
	return visibilityManager
}

func TestPrepareQueryChasmWorkflows(t *testing.T) {
	t.Parallel()

	baseFilter := fmt.Sprintf(
		"TemporalNamespaceDivision is null or TemporalNamespaceDivision = '%d'",
		chasm.WorkflowArchetypeID,
	)
	wrappedFilter := "(" + baseFilter + ")"

	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "empty query returns base filter",
			query:    "",
			expected: baseFilter,
		},
		{
			name:     "whitespace only returns base filter",
			query:    "   \t  ",
			expected: baseFilter,
		},
		{
			name:     "simple where clause is wrapped",
			query:    "foo = 'bar'",
			expected: wrappedFilter + " and (foo = 'bar')",
		},
		{
			name:     "compound where clause is wrapped",
			query:    "foo = 1 and bar = 2",
			expected: wrappedFilter + " and (foo = 1 and bar = 2)",
		},
		{
			name:     "where clause with order by",
			query:    "foo > 1 order by bar desc",
			expected: wrappedFilter + " and (foo > 1) order by bar desc",
		},
		{
			name:     "short where clause is wrapped",
			query:    "a = 1",
			expected: wrappedFilter + " and (a = 1)",
		},
		{
			name:     "query gets trimmed before wrapping",
			query:    "  foo = 'bar'  ",
			expected: wrappedFilter + " and (foo = 'bar')",
		},
		{
			name:     "order by only is appended without wrapping",
			query:    "order by foo desc",
			expected: baseFilter + " order by foo desc",
		},
		{
			name:     "group by only is appended without wrapping",
			query:    "group by foo",
			expected: baseFilter + " group by foo",
		},
		{
			name:     "order by prefix is case-insensitive",
			query:    "ORDER BY foo DESC",
			expected: baseFilter + " ORDER BY foo DESC",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := prepareQueryChasmWorkflows(tc.query)
			require.NoError(t, err)
			require.Equal(t, tc.expected, got)
		})
	}

	t.Run("invalid query returns parse error", func(t *testing.T) {
		t.Parallel()

		got, err := prepareQueryChasmWorkflows("foo === bar")
		require.Error(t, err)
		require.Empty(t, got)
	})
}

func makeResponseWithScheduledWorkflowAttributes(nameValueMap map[string]string) *schedulespb.DescribeResponse {
	attributes := commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{},
	}
	for name, value := range nameValueMap {
		attributes.IndexedFields["AliasFor"+name] = payload.EncodeString(value)
	}

	response := schedulespb.DescribeResponse{
		Schedule: &schedulepb.Schedule{
			Action: &schedulepb.ScheduleAction{
				Action: &schedulepb.ScheduleAction_StartWorkflow{
					StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
						SearchAttributes: &attributes,
					},
				},
			},
		},
	}
	return &response
}
