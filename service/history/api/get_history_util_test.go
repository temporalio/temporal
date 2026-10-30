package api

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.uber.org/mock/gomock"
)

func TestShouldIncludeTransientOrSpeculativeTasks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		clientName    string
		historySuffix []*historypb.HistoryEvent
		expected      bool
	}{
		{
			name:       "all conditions met",
			clientName: headers.ClientNameGoSDK,
			historySuffix: []*historypb.HistoryEvent{
				{
					EventId:   10,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				},
			},
			expected: true,
		},
		{
			name:          "empty history suffix",
			clientName:    headers.ClientNameGoSDK,
			historySuffix: []*historypb.HistoryEvent{},
			expected:      false,
		},
		{
			name:          "nil history suffix",
			clientName:    headers.ClientNameGoSDK,
			historySuffix: nil,
			expected:      false,
		},
		{
			name:       "CLI client",
			clientName: headers.ClientNameCLI,
			historySuffix: []*historypb.HistoryEvent{
				{
					EventId:   10,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				},
			},
			expected: false,
		},
		{
			name:       "UI client",
			clientName: headers.ClientNameUI,
			historySuffix: []*historypb.HistoryEvent{
				{
					EventId:   10,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				},
			},
			expected: false,
		},
		{
			name:       "invalid events - first event not scheduled",
			clientName: headers.ClientNameGoSDK,
			historySuffix: []*historypb.HistoryEvent{
				{
					EventId:   10,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				},
			},
			expected: false,
		},
		{
			name:       "two valid events",
			clientName: headers.ClientNameGoSDK,
			historySuffix: []*historypb.HistoryEvent{
				{
					EventId:   10,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				},
				{
					EventId:   11,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := require.New(t)

			ctx := headers.SetVersionsForTests(context.Background(), "1.0.0", tt.clientName, "", "")
			tranOrSpecEvents := &historyspb.TransientWorkflowTaskInfo{
				HistorySuffix: tt.historySuffix,
			}

			result := shouldIncludeTransientOrSpeculativeTasks(ctx, tranOrSpecEvents)
			r.Equal(tt.expected, result)
		})
	}
}

func TestClientSupportsTranOrSpecTasks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		clientName string
		expected   bool
	}{
		{
			name:       "CLI client returns false",
			clientName: headers.ClientNameCLI,
			expected:   false,
		},
		{
			name:       "UI client returns false",
			clientName: headers.ClientNameUI,
			expected:   false,
		},
		{
			name:       "Go SDK returns true",
			clientName: headers.ClientNameGoSDK,
			expected:   true,
		},
		{
			name:       "Java SDK returns true",
			clientName: headers.ClientNameJavaSDK,
			expected:   true,
		},
		{
			name:       "TypeScript SDK returns true",
			clientName: headers.ClientNameTypeScriptSDK,
			expected:   true,
		},
		{
			name:       "Python SDK returns true",
			clientName: headers.ClientNamePythonSDK,
			expected:   true,
		},
		{
			name:       "PHP SDK returns true",
			clientName: headers.ClientNamePHPSDK,
			expected:   true,
		},
		{
			name:       "no client name returns true",
			clientName: "",
			expected:   true,
		},
		{
			name:       "unknown client returns true",
			clientName: "unknown-client",
			expected:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := require.New(t)

			ctx := headers.SetVersionsForTests(context.Background(), "1.0.0", tt.clientName, "", "")
			result := ClientSupportsTranOrSpecEvents(ctx)
			r.Equal(tt.expected, result)
		})
	}

	// Test empty context without any headers set
	t.Run("empty context without headers returns true", func(t *testing.T) {
		t.Parallel()
		r := require.New(t)

		ctx := context.Background()
		result := ClientSupportsTranOrSpecEvents(ctx)
		r.True(result)
	})
}

func TestAreValidTransientOrSpecTasks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		historySuffix []*historypb.HistoryEvent
		expected      bool
	}{
		{
			name: "single scheduled event",
			historySuffix: []*historypb.HistoryEvent{
				{
					EventId:   10,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				},
			},
			expected: true,
		},
		{
			name: "two events with consecutive IDs",
			historySuffix: []*historypb.HistoryEvent{
				{
					EventId:   10,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				},
				{
					EventId:   11,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
				},
			},
			expected: true,
		},
		{
			name:          "empty events",
			historySuffix: []*historypb.HistoryEvent{},
			expected:      false,
		},
		{
			name:          "nil events",
			historySuffix: nil,
			expected:      false,
		},
		{
			name: "more than two events",
			historySuffix: []*historypb.HistoryEvent{
				{
					EventId:   10,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				},
				{
					EventId:   11,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
				},
				{
					EventId:   12,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
				},
			},
			expected: false,
		},
		{
			name: "first event not scheduled",
			historySuffix: []*historypb.HistoryEvent{
				{
					EventId:   10,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
				},
			},
			expected: false,
		},
		{
			name: "second event not started",
			historySuffix: []*historypb.HistoryEvent{
				{
					EventId:   10,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				},
				{
					EventId:   11,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
				},
			},
			expected: false,
		},
		{
			name: "non-consecutive event IDs",
			historySuffix: []*historypb.HistoryEvent{
				{
					EventId:   10,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				},
				{
					EventId:   12,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
				},
			},
			expected: false,
		},
		{
			name: "same event IDs",
			historySuffix: []*historypb.HistoryEvent{
				{
					EventId:   10,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				},
				{
					EventId:   10,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := require.New(t)

			tranOrSpecEvents := &historyspb.TransientWorkflowTaskInfo{
				HistorySuffix: tt.historySuffix,
			}

			result := areValidTransientOrSpecEvents(tranOrSpecEvents)
			r.Equal(tt.expected, result)
		})
	}
}

func TestProcessOutgoingSearchAttributes(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	visibilityMgr := manager.NewMockVisibilityManager(ctrl)
	visibilityMgr.EXPECT().GetIndexName().Return("").AnyTimes()

	newPayload := func(value string) *commonpb.Payload {
		return payload.EncodeString(value)
	}
	unaliasedStartedEvent := &historypb.HistoryEvent{
		EventId:   1,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
				SearchAttributes: &commonpb.SearchAttributes{
					IndexedFields: map[string]*commonpb.Payload{
						"Keyword01": newPayload("value1"),
					},
				},
			},
		},
	}
	aliasedUpsertEvent := &historypb.HistoryEvent{
		EventId:   2,
		EventType: enumspb.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES,
		Attributes: &historypb.HistoryEvent_UpsertWorkflowSearchAttributesEventAttributes{
			UpsertWorkflowSearchAttributesEventAttributes: &historypb.UpsertWorkflowSearchAttributesEventAttributes{
				SearchAttributes: &commonpb.SearchAttributes{
					IndexedFields: map[string]*commonpb.Payload{
						sadefs.TemporalAliasedSearchAttributes: sadefs.TemporalAliasedSearchAttributesPayload,
						"AliasForKeyword01":                    newPayload("value2"),
						sadefs.TemporalChangeVersion:           newPayload("change"),
					},
				},
			},
		},
	}
	noSearchAttributesEvent := &historypb.HistoryEvent{
		EventId:   3,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{
			WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{},
		},
	}

	err := ProcessOutgoingSearchAttributes(
		searchattribute.NewTestProvider(),
		searchattribute.NewTestMapperProvider(&searchattribute.TestMapper{}),
		[]*historypb.HistoryEvent{unaliasedStartedEvent, aliasedUpsertEvent, noSearchAttributesEvent},
		namespace.Name("test-namespace"),
		visibilityMgr,
	)
	require.NoError(t, err)

	startedSAs := unaliasedStartedEvent.GetWorkflowExecutionStartedEventAttributes().GetSearchAttributes().GetIndexedFields()
	require.Len(t, startedSAs, 1)
	require.Contains(t, startedSAs, "AliasForKeyword01")
	require.Equal(t, "Keyword", string(startedSAs["AliasForKeyword01"].GetMetadata()[searchattribute.MetadataType]))

	upsertSAs := aliasedUpsertEvent.GetUpsertWorkflowSearchAttributesEventAttributes().GetSearchAttributes().GetIndexedFields()
	require.Len(t, upsertSAs, 2)
	require.Contains(t, upsertSAs, "AliasForKeyword01")
	require.Equal(t, "Keyword", string(upsertSAs["AliasForKeyword01"].GetMetadata()[searchattribute.MetadataType]))
	require.Equal(t, "KeywordList", string(upsertSAs[sadefs.TemporalChangeVersion].GetMetadata()[searchattribute.MetadataType]))
}
