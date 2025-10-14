package serialization

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

func TestProtoEncode(t *testing.T) {
	// only testing edge cases here; happy path is covered plenty already

	t.Run("nil message", func(t *testing.T) {
		blob, err := ProtoEncode(nil)
		require.NoError(t, err)
		require.NotNil(t, blob)
		assert.Equal(t, enumspb.ENCODING_TYPE_PROTO3, blob.EncodingType)
		assert.Nil(t, blob.Data)
	})

	t.Run("nil pointer message", func(t *testing.T) {
		var shardInfo *persistencespb.ShardInfo
		blob, err := ProtoEncode(shardInfo)
		require.NoError(t, err)
		require.NotNil(t, blob)
		assert.Equal(t, enumspb.ENCODING_TYPE_PROTO3, blob.EncodingType)
		assert.Nil(t, blob.Data)
	})
}

func TestProtoDecode(t *testing.T) {
	// only testing edge cases here; happy path is covered plenty already

	t.Run("nil data blob", func(t *testing.T) {
		var result persistencespb.ShardInfo
		err := Decode(nil, &result)
		require.Error(t, err)
		assert.IsType(t, &DeserializationError{}, err)
		assert.Contains(t, err.Error(), "cannot decode nil")
	})

	t.Run("empty data blob", func(t *testing.T) {
		blob := &commonpb.DataBlob{}

		var result persistencespb.ShardInfo
		err := Decode(blob, &result)
		require.Error(t, err)
		assert.IsType(t, &UnknownEncodingTypeError{}, err)
		assert.Contains(t, err.Error(), "unknown or unsupported encoding type Unspecified")
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
		assert.IsType(t, &UnknownEncodingTypeError{}, err)
		assert.Contains(t, err.Error(), "unknown or unsupported encoding type 999")
	})

	t.Run("invalid proto data", func(t *testing.T) {
		blob := &commonpb.DataBlob{
			Data:         []byte("invalid proto data"),
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		}

		var result persistencespb.ShardInfo
		err := Decode(blob, &result)
		require.Error(t, err)
		assert.IsType(t, &DeserializationError{}, err)
		assert.Contains(t, err.Error(), "error deserializing using Proto3 encoding")
	})
}
