package serialization

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

func TestEncode(t *testing.T) {
	// only testing edge cases here; happy path is covered plenty already

	t.Run("nil message", func(t *testing.T) {
		t.Setenv(SerializerDataEncodingEnvVar, "proto3")
		blob, err := Encode(nil)
		require.NoError(t, err)
		require.NotNil(t, blob)
		require.Equal(t, enumspb.ENCODING_TYPE_PROTO3, blob.EncodingType)
		require.Nil(t, blob.Data)
	})

	t.Run("nil pointer message", func(t *testing.T) {
		t.Setenv(SerializerDataEncodingEnvVar, "proto3")
		var shardInfo *persistencespb.ShardInfo
		blob, err := Encode(shardInfo)
		require.NoError(t, err)
		require.NotNil(t, blob)
		require.Equal(t, enumspb.ENCODING_TYPE_PROTO3, blob.EncodingType)
		require.Nil(t, blob.Data)
	})

	t.Run("respects env encoding", func(t *testing.T) {
		t.Setenv(SerializerDataEncodingEnvVar, "json")
		blob, err := Encode(nil)
		require.NoError(t, err)
		require.NotNil(t, blob)
		require.Equal(t, enumspb.ENCODING_TYPE_JSON, blob.EncodingType)
		require.Nil(t, blob.Data)
	})
}

func TestProtoDecode(t *testing.T) {
	// only testing edge cases here; happy path is covered plenty already

	t.Run("nil data blob", func(t *testing.T) {
		var result persistencespb.ShardInfo
		err := Decode(nil, &result)
		require.Error(t, err)
		var deserializationErr *DeserializationError
		require.ErrorAs(t, err, &deserializationErr)
		require.Contains(t, err.Error(), "cannot decode nil")
	})

	t.Run("empty data blob", func(t *testing.T) {
		blob := &commonpb.DataBlob{}

		var result persistencespb.ShardInfo
		err := Decode(blob, &result)
		require.Error(t, err)
		var unknownEncodingErr *UnknownEncodingTypeError
		require.ErrorAs(t, err, &unknownEncodingErr)
		require.Contains(t, err.Error(), "unknown or unsupported encoding type Unspecified")
	})

	t.Run("nil data field", func(t *testing.T) {
		blob := &commonpb.DataBlob{
			Data:         nil,
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		}

		var result persistencespb.ShardInfo
		err := Decode(blob, &result)
		require.NoError(t, err)
	})

	t.Run("unknown encoding type", func(t *testing.T) {
		blob := &commonpb.DataBlob{
			Data:         []byte("some data"),
			EncodingType: enumspb.EncodingType(999),
		}

		var result persistencespb.ShardInfo
		err := Decode(blob, &result)
		require.Error(t, err)
		var unknownEncodingErr *UnknownEncodingTypeError
		require.ErrorAs(t, err, &unknownEncodingErr)
		require.Contains(t, err.Error(), "unknown or unsupported encoding type 999")
	})

	t.Run("invalid proto data", func(t *testing.T) {
		blob := &commonpb.DataBlob{
			Data:         []byte("invalid proto data"),
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		}

		var result persistencespb.ShardInfo
		err := Decode(blob, &result)
		require.Error(t, err)
		var deserializationErr *DeserializationError
		require.ErrorAs(t, err, &deserializationErr)
		require.Contains(t, err.Error(), "error deserializing using Proto3 encoding")
	})
}
