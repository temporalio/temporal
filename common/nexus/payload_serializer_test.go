package nexus

import (
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
)

func mustToPayload(t *testing.T, v any) *commonpb.Payload {
	conv := converter.GetDefaultDataConverter()
	payload, err := conv.ToPayload(v)
	require.NoError(t, err)
	return payload
}

func TestNexusPayloadSerializer(t *testing.T) {
	t.Parallel()

	type testcase struct {
		name         string
		inputPayload *commonpb.Payload
		// defaults to inputPayload
		expectedPayload *commonpb.Payload
		header          nexus.Header
	}
	// DO NOT SUBMIT: fix callers to work with a pointer (go/goprotoapi-findings#message-value)
	cases := []testcase{
		{
			name:         "json",
			inputPayload: mustToPayload(t, "foo"),
			header:       nexus.Header{"type": "application/json"},
		},
		{
			name:         "bytes",
			inputPayload: mustToPayload(t, []byte("foo")),
			header:       nexus.Header{"type": "application/octet-stream"},
		},
		{
			name:         "nil",
			inputPayload: mustToPayload(t, nil),
			header:       nexus.Header{},
		},
		{
			// Empty payload is preserved.
			name:            "empty",
			inputPayload:    &commonpb.Payload{},
			expectedPayload: &commonpb.Payload{},
			header:          nexus.Header{"type": "application/x-temporal-payload"},
		},
		{
			name:         "json proto",
			inputPayload: mustToPayload(t, &commonpb.RetryPolicy{}),
			header: nexus.Header{
				"type": `application/json; format=protobuf; message-type="temporal.api.common.v1.RetryPolicy"`,
			},
		},
		{
			name: "binary proto with no messageType",
			inputPayload: commonpb.Payload_builder{
				Data: []byte("ignored"),
				Metadata: map[string][]byte{
					"encoding": []byte("binary/protobuf"),
				},
			}.Build(),
			header: nexus.Header{
				"type": "application/x-temporal-payload",
			},
		},
		{
			name: "binary proto with messageType",
			inputPayload: commonpb.Payload_builder{
				Data: []byte("ignored"),
				Metadata: map[string][]byte{
					"encoding":    []byte("binary/protobuf"),
					"messageType": []byte("temporal.api.common.v1.RetryPolicy"),
				},
			}.Build(),
			header: nexus.Header{
				"type": `application/x-protobuf; message-type="temporal.api.common.v1.RetryPolicy"`,
			},
		},
		{
			name:         "nil pointer",
			inputPayload: nil,
			expectedPayload: commonpb.Payload_builder{
				Metadata: map[string][]byte{
					"encoding": []byte("binary/null"),
				},
			}.Build(),
			header: nexus.Header{},
		},
		{
			name: "nil metadata non-empty data",
			inputPayload: commonpb.Payload_builder{
				Data: []byte("not empty"),
			}.Build(),
			expectedPayload: commonpb.Payload_builder{
				Metadata: map[string][]byte{},
				Data:     []byte("not empty"),
			}.Build(),
			header: nexus.Header{"type": "application/x-temporal-payload"},
		},
		{
			name: "non-standard encoding",
			inputPayload: commonpb.Payload_builder{
				Data: []byte("some-data"),
				Metadata: map[string][]byte{
					"encoding": []byte("non-standard"),
				},
			}.Build(),
			header: nexus.Header{"type": "application/x-temporal-payload"},
		},
		{
			name: "non-standard metadata field",
			inputPayload: commonpb.Payload_builder{
				Data: []byte("some-data"),
				Metadata: map[string][]byte{
					"encoding":     []byte("binary/plain"),
					"non-standard": []byte("value"),
				},
			}.Build(),
			header: nexus.Header{"type": "application/x-temporal-payload"},
		},
		{
			name: "nexus content with non-standard header",
			inputPayload: commonpb.Payload_builder{
				Metadata: map[string][]byte{
					"encoding":     []byte("unknown/nexus-content"),
					"type":         []byte("application/json"),
					"non-standard": []byte("value"),
				},
				Data: []byte(`"data"`),
			}.Build(),
			header: nexus.Header{"non-standard": "value", "type": "application/json"},
		},
		{
			name: "nexus content with non-standard content params",
			inputPayload: commonpb.Payload_builder{
				Metadata: map[string][]byte{
					"encoding": []byte("unknown/nexus-content"),
					"type":     []byte("application/json; something=nonstandard"),
				},
				Data: []byte(`"data"`),
			}.Build(),
			header: nexus.Header{"type": "application/json; something=nonstandard"},
		},
		{
			name: "nexus content with non-standard media type",
			inputPayload: commonpb.Payload_builder{
				Metadata: map[string][]byte{
					"encoding": []byte("unknown/nexus-content"),
					"type":     []byte("application/x-www-form-urlencoded"),
				},
				Data: []byte(`"data"`),
			}.Build(),
			header: nexus.Header{"type": "application/x-www-form-urlencoded"},
		},
		{
			name: "nexus content with unparsable content params",
			inputPayload: commonpb.Payload_builder{
				Metadata: map[string][]byte{
					"encoding": []byte("unknown/nexus-content"),
					"type":     []byte("application/"),
				},
				Data: []byte(`"data"`),
			}.Build(),
			header: nexus.Header{"type": "application/"},
		},
		{
			name: "nexus content with length header",
			inputPayload: commonpb.Payload_builder{
				Metadata: map[string][]byte{
					"encoding": []byte("unknown/nexus-content"),
					"type":     []byte("application/json"),
					"length":   []byte("4"),
				},
				Data: []byte(`"data"`),
			}.Build(),
			expectedPayload: commonpb.Payload_builder{
				Metadata: map[string][]byte{
					"encoding": []byte("json/plain"),
				},
				Data: []byte(`"data"`),
			}.Build(),
			header: nexus.Header{"type": "application/json", "length": "4"},
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			s := payloadSerializer{}
			content, err := s.Serialize(c.inputPayload)
			require.NoError(t, err)
			require.Equal(t, c.header, content.Header)
			var outputPayload *commonpb.Payload
			require.NoError(t, s.Deserialize(content, &outputPayload))
			expectedPayload := c.expectedPayload
			if expectedPayload == nil {
				expectedPayload = c.inputPayload
			}
			require.True(t, expectedPayload.Equal(outputPayload))
		})
	}
}
