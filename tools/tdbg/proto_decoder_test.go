package tdbg

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/common/codec"
	"google.golang.org/protobuf/proto"
)

func TestDecodePayloadsInJSON(t *testing.T) {
	mustMarshal := func(msg proto.Message) []byte {
		t.Helper()
		data, err := proto.Marshal(msg)
		require.NoError(t, err)
		return data
	}

	nestedInnerData := mustMarshal(&commonpb.WorkflowType{Name: "nested-payload"})

	events := []*historypb.HistoryEvent{
		{
			EventId:   1,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{
				WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
					SignalName: "my-signal",
					Input: &commonpb.Payloads{
						Payloads: []*commonpb.Payload{
							// 1.1: binary/protobuf, known type.
							{
								Metadata: map[string][]byte{
									"encoding":    []byte("binary/protobuf"),
									"messageType": []byte("temporal.api.common.v1.WorkflowType"),
								},
								Data: mustMarshal(&commonpb.WorkflowType{Name: "decoded-signal"}),
							},
							// 1.2: binary/protobuf, nested Payloads containing an inner payload.
							{
								Metadata: map[string][]byte{
									"encoding":    []byte("binary/protobuf"),
									"messageType": []byte("temporal.api.common.v1.Payloads"),
								},
								Data: mustMarshal(&commonpb.Payloads{
									Payloads: []*commonpb.Payload{
										{
											Metadata: map[string][]byte{
												"encoding":    []byte("binary/protobuf"),
												"messageType": []byte("temporal.api.common.v1.WorkflowType"),
											},
											Data: nestedInnerData,
										},
									},
								}),
							},
						},
					},
				},
			},
		},
		{
			EventId:   2,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
				WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{
					Result: &commonpb.Payloads{
						Payloads: []*commonpb.Payload{
							// 2.1: binary/protobuf, known type.
							{
								Metadata: map[string][]byte{
									"encoding":    []byte("binary/protobuf"),
									"messageType": []byte("temporal.api.common.v1.WorkflowType"),
								},
								Data: mustMarshal(&commonpb.WorkflowType{Name: "completed-result"}),
							},
							// 2.2: binary/protobuf, unknown type — data left as base64.
							{
								Metadata: map[string][]byte{
									"encoding":    []byte("binary/protobuf"),
									"messageType": []byte("some.unknown.Type"),
								},
								Data: mustMarshal(&commonpb.WorkflowType{Name: "unknown"}),
							},
							// 2.3: json/plain — no messageType, data is JSON text.
							{
								Metadata: map[string][]byte{
									"encoding": []byte("json/plain"),
								},
								Data: []byte(`{"key":"value"}`),
							},
							// 2.4: binary/plain — no messageType, data left as base64.
							{
								Metadata: map[string][]byte{
									"encoding": []byte("binary/plain"),
								},
								Data: []byte("opaque binary data"),
							},
						},
					},
				},
			},
		},
	}

	encoder := codec.NewJSONPBEncoder()
	data, err := encoder.EncodeHistoryEvents(events)
	require.NoError(t, err)

	decoded := decodePayloadsInJSON(data)

	var parsed []map[string]any
	require.NoError(t, json.Unmarshal(decoded, &parsed))
	require.Len(t, parsed, 2)

	// === Event 1: Signal

	evtAttrs1 := parsed[0]["workflowExecutionSignaledEventAttributes"].(map[string]any)
	evtPayloads1 := evtAttrs1["input"].(map[string]any)["payloads"].([]any)
	require.Len(t, evtPayloads1, 2)

	// 1.1: binary/protobuf, known type — metadata decoded, data decoded to JSON.
	assert.Equal(t, map[string]any{
		"metadata": map[string]any{
			"encoding":    "binary/protobuf",
			"messageType": "temporal.api.common.v1.WorkflowType",
		},
		"data": map[string]any{"name": "decoded-signal"},
	}, evtPayloads1[0])

	// 1.2: binary/protobuf, nested — outer decoded, inner payload also decoded.
	assert.Equal(t, map[string]any{
		"metadata": map[string]any{
			"encoding":    "binary/protobuf",
			"messageType": "temporal.api.common.v1.Payloads",
		},
		"data": map[string]any{
			"payloads": []any{
				map[string]any{
					"metadata": map[string]any{
						"encoding":    "binary/protobuf",
						"messageType": "temporal.api.common.v1.WorkflowType",
					},
					"data": map[string]any{"name": "nested-payload"},
				},
			},
		},
	}, evtPayloads1[1])

	// === Event 2: WorkflowExecutionCompleted

	evtAttrs2 := parsed[1]["workflowExecutionCompletedEventAttributes"].(map[string]any)
	evtPayloads2 := evtAttrs2["result"].(map[string]any)["payloads"].([]any)
	require.Len(t, evtPayloads2, 4)

	// 2.1: binary/protobuf, known type — decoded.
	assert.Equal(t, map[string]any{
		"metadata": map[string]any{
			"encoding":    "binary/protobuf",
			"messageType": "temporal.api.common.v1.WorkflowType",
		},
		"data": map[string]any{"name": "completed-result"},
	}, evtPayloads2[0])

	// 2.2: binary/protobuf, unknown type — messageType decoded but data left as base64.
	assert.Equal(t, map[string]any{
		"metadata": map[string]any{
			"encoding":    "binary/protobuf",
			"messageType": "some.unknown.Type",
		},
		"data": "Cgd1bmtub3du",
	}, evtPayloads2[1])

	// 2.3: json/plain — data is JSON, decoded and inlined.
	assert.Equal(t, map[string]any{
		"metadata": map[string]any{
			"encoding": "json/plain",
		},
		"data": map[string]any{"key": "value"},
	}, evtPayloads2[2])

	// 2.4: binary/plain — metadata decoded, data left as base64.
	assert.Equal(t, map[string]any{
		"metadata": map[string]any{
			"encoding": "binary/plain",
		},
		"data": "b3BhcXVlIGJpbmFyeSBkYXRh",
	}, evtPayloads2[3])
}

func TestDecodePayloadsInJSON_InvalidInput(t *testing.T) {
	invalidJSON := []byte(`not json`)
	result := decodePayloadsInJSON(invalidJSON)
	assert.Equal(t, invalidJSON, result) //nolint:testifylint // input is intentionally not valid JSON
}
