package payload

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestConverterMatchesSDKDefaultDataConverter(t *testing.T) {
	sdk := converter.GetDefaultDataConverter()
	memo := &commonpb.Memo{Fields: map[string]*commonpb.Payload{"k": EncodeString("v")}}
	values := []any{
		nil,
		(*testStruct)(nil),
		(*commonpb.Memo)(nil),
		[]byte{1, 2, 3},
		[]byte(nil),
		"str",
		10,
		3.5,
		true,
		time.Unix(1700000000, 5).UTC(),
		[]string(nil),
		[]string{},
		[]string{"a", "b"},
		map[string]any{"a": 1, "b": []int{2}},
		testStruct{Int: 1, String: "s", Bytes: []byte{4}},
		&testStruct{Int: 1, String: "s", Bytes: []byte{4}},
		memo,
		durationpb.New(time.Second),
		func() {},
	}
	for _, v := range values {
		t.Run(fmt.Sprintf("%T", v), func(t *testing.T) {
			expected, expectedErr := sdk.ToPayload(v)
			actual, actualErr := Encode(v)
			requireSameError(t, expectedErr, actualErr)
			require.True(t, proto.Equal(expected, actual), "expected %v, got %v", expected, actual)
			require.Equal(t, sdk.ToString(expected), ToString(actual))
		})
	}
}

func TestDecodeMatchesSDKDefaultDataConverter(t *testing.T) {
	sdk := converter.GetDefaultDataConverter()
	memo := &commonpb.Memo{Fields: map[string]*commonpb.Payload{"k": EncodeString("v")}}
	memoBinary, err := converter.NewProtoPayloadConverter().ToPayload(memo)
	require.NoError(t, err)
	payloads := []*commonpb.Payload{
		nil,
		{},
		{Metadata: map[string][]byte{}},
		{Metadata: map[string][]byte{metadataEncoding: []byte("unknown")}},
		mustEncode(t, nil),
		mustEncode(t, []byte{1, 2, 3}),
		mustEncode(t, "str"),
		mustEncode(t, 10),
		mustEncode(t, []string{"a"}),
		mustEncode(t, memo),
		{Metadata: map[string][]byte{metadataEncoding: []byte(encodingProtoJSON)}, Data: []byte("null")},
		{Metadata: map[string][]byte{metadataEncoding: []byte(encodingProtoJSON)}, Data: []byte("{")},
		memoBinary,
	}
	targets := []func() any{
		func() any { return new(string) },
		func() any { return new(int) },
		func() any { return new([]string) },
		func() any { return new([]byte) },
		func() any { return new(any) },
		func() any { return new(*string) },
		func() any { return new(*commonpb.Memo) },
		func() any { return &commonpb.Memo{} },
		func() any { return new(commonpb.Memo) },
		func() any { return "not a pointer" },
	}
	for i, p := range payloads {
		for j, newTarget := range targets {
			t.Run(fmt.Sprintf("%d/%d", i, j), func(t *testing.T) {
				expected, actual := newTarget(), newTarget()
				expectedErr := sdk.FromPayload(p, expected)
				actualErr := Decode(p, actual)
				requireSameError(t, expectedErr, actualErr)
				if m, ok := expected.(proto.Message); ok {
					require.True(t, proto.Equal(m, actual.(proto.Message)))
				} else if m, ok := expected.(**commonpb.Memo); ok {
					require.True(t, proto.Equal(*m, *actual.(**commonpb.Memo)))
				} else {
					require.Equal(t, expected, actual)
				}
				require.Equal(t, sdk.ToString(p), ToString(p))
			})
		}
	}
}

func mustEncode(t *testing.T, v any) *commonpb.Payload {
	p, err := Encode(v)
	require.NoError(t, err)
	return p
}

func requireSameError(t *testing.T, expected, actual error) {
	if expected == nil {
		require.NoError(t, actual)
		return
	}
	require.EqualError(t, actual, expected.Error())
}
