// The MIT License
//
// Copyright (c) 2023 Temporal Technologies Inc.  All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
			name:         "json proto",
			inputPayload: mustToPayload(t, commonpb.RetryPolicy{}),
			header: nexus.Header{
				"type": `application/json; format=protobuf; message-type="temporal.api.common.v1.RetryPolicy"`,
			},
		},
		{
			name: "binary proto with no messageType",
			inputPayload: &commonpb.Payload{
				Data: []byte("ignored"),
				Metadata: map[string][]byte{
					"encoding": []byte("binary/protobuf"),
				},
			},
			header: nexus.Header{
				"type": "application/x-temporal-payload",
			},
		},
		{
			name: "binary proto with messageType",
			inputPayload: &commonpb.Payload{
				Data: []byte("ignored"),
				Metadata: map[string][]byte{
					"encoding":    []byte("binary/protobuf"),
					"messageType": []byte("temporal.api.common.v1.RetryPolicy"),
				},
			},
			header: nexus.Header{
				"type": `application/x-protobuf; message-type="temporal.api.common.v1.RetryPolicy"`,
			},
		},
		{
			name:         "nil pointer",
			inputPayload: nil,
			expectedPayload: &commonpb.Payload{
				Metadata: map[string][]byte{
					"encoding": []byte("binary/null"),
				},
			},
			// Yes this is the default value, but this test should have an explicit expectation.
			header: nil,
		},
		{
			name:         "nil metadata",
			inputPayload: &commonpb.Payload{},
			expectedPayload: &commonpb.Payload{
				Metadata: map[string][]byte{},
				Data:     []byte{},
			},
			header: nexus.Header{"type": "application/x-temporal-payload"},
		},
		{
			name: "non-standard encoding",
			inputPayload: &commonpb.Payload{
				Data: []byte("some-data"),
				Metadata: map[string][]byte{
					"encoding": []byte("non-standard"),
				},
			},
			header: nexus.Header{"type": "application/x-temporal-payload"},
		},
		{
			name: "non-standard metadata field",
			inputPayload: &commonpb.Payload{
				Data: []byte("some-data"),
				Metadata: map[string][]byte{
					"encoding":     []byte("binary/plain"),
					"non-standard": []byte("value"),
				},
			},
			header: nexus.Header{"type": "application/x-temporal-payload"},
		},
		{
			name: "nexus content with non-standard header",
			inputPayload: &commonpb.Payload{
				Metadata: map[string][]byte{
					"encoding":     []byte("unknown/nexus-content"),
					"type":         []byte("application/json"),
					"non-standard": []byte("value"),
				},
				Data: []byte(`"data"`),
			},
			header: nexus.Header{"non-standard": "value", "type": "application/json"},
		},
		{
			name: "nexus content with non-standard content params",
			inputPayload: &commonpb.Payload{
				Metadata: map[string][]byte{
					"encoding": []byte("unknown/nexus-content"),
					"type":     []byte("application/json; something=nonstandard"),
				},
				Data: []byte(`"data"`),
			},
			header: nexus.Header{"type": "application/json; something=nonstandard"},
		},
		{
			name: "nexus content with non-standard media type",
			inputPayload: &commonpb.Payload{
				Metadata: map[string][]byte{
					"encoding": []byte("unknown/nexus-content"),
					"type":     []byte("application/x-www-form-urlencoded"),
				},
				Data: []byte(`"data"`),
			},
			header: nexus.Header{"type": "application/x-www-form-urlencoded"},
		},
		{
			name: "nexus content with unparsable content params",
			inputPayload: &commonpb.Payload{
				Metadata: map[string][]byte{
					"encoding": []byte("unknown/nexus-content"),
					"type":     []byte("application/"),
				},
				Data: []byte(`"data"`),
			},
			header: nexus.Header{"type": "application/"},
		},
		{
			name: "nexus content with length header",
			inputPayload: &commonpb.Payload{
				Metadata: map[string][]byte{
					"encoding": []byte("unknown/nexus-content"),
					"type":     []byte("application/json"),
					"length":   []byte("4"),
				},
				Data: []byte(`"data"`),
			},
			expectedPayload: &commonpb.Payload{
				Metadata: map[string][]byte{
					"encoding": []byte("json/plain"),
				},
				Data: []byte(`"data"`),
			},
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
