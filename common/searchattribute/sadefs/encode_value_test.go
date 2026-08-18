package sadefs

import (
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/server/common/payload"
)

func Test_DecodeValue_Success(t *testing.T) {
	testCases := []struct {
		name      string
		value     any
		valueType enumspb.IndexedValueType
	}{
		{
			name:      "nil",
			value:     nil,
			valueType: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
		{
			name:      "bool",
			value:     true,
			valueType: enumspb.INDEXED_VALUE_TYPE_BOOL,
		},
		{
			name:      "datetime",
			value:     time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC),
			valueType: enumspb.INDEXED_VALUE_TYPE_DATETIME,
		},
		{
			name:      "double",
			value:     1.23,
			valueType: enumspb.INDEXED_VALUE_TYPE_DOUBLE,
		},
		{
			name:      "int",
			value:     int64(123),
			valueType: enumspb.INDEXED_VALUE_TYPE_INT,
		},
		{
			name:      "keyword",
			value:     "foo",
			valueType: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
		{
			name:      "keyword list",
			value:     []string{"foo", "bar"},
			valueType: enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
		},
		{
			name:      "text",
			value:     "foo bar",
			valueType: enumspb.INDEXED_VALUE_TYPE_TEXT,
		},
	}

	for _, tc := range testCases {
		for _, setMetadata := range []bool{false, true} {
			for _, allowList := range []bool{false, true} {
				tcName := fmt.Sprintf("%s/setMetadata=%v/allowList=%v", tc.name, setMetadata, allowList)
				t.Run(tcName, func(t *testing.T) {
					encodedValue, err := payload.Encode(tc.value)
					require.NoError(t, err)
					valueType := tc.valueType
					if setMetadata {
						encodedValue.Metadata[MetadataType] = []byte(tc.valueType.String())
						valueType = enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED
					}
					decodedValue, err := DecodeValue(encodedValue, valueType, allowList)
					require.NoError(t, err)
					require.Equal(t, tc.value, decodedValue)
				})
			}
		}
	}

	t.Run("from parameter ignore metadata", func(t *testing.T) {
		value := int64(123)
		encodedValue, err := payload.Encode(value)
		require.NoError(t, err)
		encodedValue.Metadata[MetadataType] = []byte("UnknownType") // MetadataType is not used.
		decodedValue, err := DecodeValue(encodedValue, enumspb.INDEXED_VALUE_TYPE_INT, false)
		require.NoError(t, err)
		require.Equal(t, value, decodedValue)
	})
}

func Test_DecodeValue_AllowList_BackwardsCompatibility(t *testing.T) {
	t.Run("list of int", func(t *testing.T) {
		value := []int64{123, 456}
		encodedValue, err := payload.Encode(value)
		require.NoError(t, err)
		decodedValue, err := DecodeValue(encodedValue, enumspb.INDEXED_VALUE_TYPE_INT, true)
		require.NoError(t, err)
		require.Equal(t, value, decodedValue)
	})

	t.Run("list of keyword", func(t *testing.T) {
		value := []string{"foo", "bar"}
		encodedValue, err := payload.Encode(value)
		require.NoError(t, err)
		decodedValue, err := DecodeValue(encodedValue, enumspb.INDEXED_VALUE_TYPE_KEYWORD, true)
		require.NoError(t, err)
		require.Equal(t, value, decodedValue)
	})

	t.Run("empty list", func(t *testing.T) {
		value := []string{}
		encodedValue, err := payload.Encode(value)
		require.NoError(t, err)
		decodedValue, err := DecodeValue(encodedValue, enumspb.INDEXED_VALUE_TYPE_KEYWORD, true)
		require.NoError(t, err)
		require.Nil(t, decodedValue)
	})

	t.Run("list with single value allowing list", func(t *testing.T) {
		value := []string{"foo"}
		encodedValue, err := payload.Encode(value)
		require.NoError(t, err)
		decodedValue, err := DecodeValue(encodedValue, enumspb.INDEXED_VALUE_TYPE_KEYWORD, true)
		require.NoError(t, err)
		require.Equal(t, value, decodedValue)
	})

	t.Run("list with single value not allowing list", func(t *testing.T) {
		value := []string{"foo"}
		encodedValue, err := payload.Encode(value)
		require.NoError(t, err)
		decodedValue, err := DecodeValue(encodedValue, enumspb.INDEXED_VALUE_TYPE_KEYWORD, false)
		require.NoError(t, err)
		require.Equal(t, "foo", decodedValue)
	})
}

func Test_DecodeValue_Error(t *testing.T) {
	t.Run("no value type set", func(t *testing.T) {
		value := int64(123)
		encodedValue, err := payload.Encode(value)
		require.NoError(t, err)
		_, err = DecodeValue(encodedValue, enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, false)
		require.ErrorIs(t, err, ErrInvalidType)
	})

	t.Run("invalid metadata type", func(t *testing.T) {
		value := int64(123)
		encodedValue, err := payload.Encode(value)
		encodedValue.Metadata[MetadataType] = []byte("UnknownType")
		require.NoError(t, err)
		_, err = DecodeValue(encodedValue, enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, false)
		require.ErrorIs(t, err, ErrInvalidType)
	})

	t.Run("unknown value type", func(t *testing.T) {
		value := int64(123)
		encodedValue, err := payload.Encode(value)
		require.NoError(t, err)
		_, err = DecodeValue(encodedValue, enumspb.IndexedValueType(999), false)
		require.ErrorIs(t, err, ErrInvalidType)
	})

	t.Run("incorrect metadata type", func(t *testing.T) {
		value := int64(123)
		encodedValue, err := payload.Encode(value)
		encodedValue.Metadata[MetadataType] = []byte("Keyword")
		require.NoError(t, err)
		_, err = DecodeValue(encodedValue, enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, false)
		require.ErrorIs(t, err, converter.ErrUnableToDecode)
	})

	t.Run("incorrect input type", func(t *testing.T) {
		value := int64(123)
		encodedValue, err := payload.Encode(value)
		require.NoError(t, err)
		_, err = DecodeValue(encodedValue, enumspb.INDEXED_VALUE_TYPE_KEYWORD, false)
		require.ErrorIs(t, err, converter.ErrUnableToDecode)
	})

	t.Run("incorrect input type with correct metadata type", func(t *testing.T) {
		value := int64(123)
		encodedValue, err := payload.Encode(value)
		encodedValue.Metadata[MetadataType] = []byte("Int")
		require.NoError(t, err)
		_, err = DecodeValue(encodedValue, enumspb.INDEXED_VALUE_TYPE_KEYWORD, false)
		require.ErrorIs(t, err, converter.ErrUnableToDecode)
	})

	t.Run("list not allowed", func(t *testing.T) {
		value := []int64{123, 456}
		encodedValue, err := payload.Encode(value)
		require.NoError(t, err)
		_, err = DecodeValue(encodedValue, enumspb.INDEXED_VALUE_TYPE_INT, false)
		require.ErrorIs(t, err, converter.ErrUnableToDecode)
	})

	t.Run("single value invalid for KeywordList", func(t *testing.T) {
		value := "foo"
		encodedValue, err := payload.Encode(value)
		require.NoError(t, err)
		_, err = DecodeValue(encodedValue, enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, true)
		require.ErrorIs(t, err, converter.ErrUnableToDecode)
	})

	t.Run("list of KeywordList never allowed", func(t *testing.T) {
		value := [][]string{{"foo", "bar"}, {"asdf", "qwer"}}
		encodedValue, err := payload.Encode(value)
		require.NoError(t, err)
		_, err = DecodeValue(encodedValue, enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, true)
		require.ErrorIs(t, err, converter.ErrUnableToDecode)
	})

	// TODO(rodrigozhou): test disabled as it an existing bug
	// t.Run("nil value not allowed in KeywordList", func(t *testing.T) {
	// 	value := []any{nil}
	// 	encodedValue, err := payload.Encode(value)
	// 	require.NoError(t, err)
	// 	_, err = DecodeValue(encodedValue, enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, true)
	// 	require.ErrorIs(t, err, converter.ErrUnableToDecode)
	// })
}

func Test_DecodeKeywordList(t *testing.T) {
	t.Run("nil payload", func(t *testing.T) {
		encodedValue, err := payload.Encode(nil)
		require.NoError(t, err)
		decodedValue, err := DecodeKeywordList(encodedValue)
		require.NoError(t, err)
		require.Nil(t, decodedValue)
	})

	t.Run("list of strings", func(t *testing.T) {
		value := []string{"foo", "bar"}
		encodedValue, err := payload.Encode(value)
		require.NoError(t, err)
		decodedValue, err := DecodeKeywordList(encodedValue)
		require.NoError(t, err)
		require.Equal(t, value, decodedValue)
	})

	t.Run("list with single value", func(t *testing.T) {
		value := []string{"foo"}
		encodedValue, err := payload.Encode(value)
		require.NoError(t, err)
		decodedValue, err := DecodeKeywordList(encodedValue)
		require.NoError(t, err)
		require.Equal(t, value, decodedValue)
	})

	t.Run("empty list", func(t *testing.T) {
		value := []string{}
		encodedValue, err := payload.Encode(value)
		require.NoError(t, err)
		decodedValue, err := DecodeKeywordList(encodedValue)
		require.NoError(t, err)
		require.Equal(t, value, decodedValue)
	})

	t.Run("error: nil value in list", func(t *testing.T) {
		value := []any{"foo", nil}
		encodedValue, err := payload.Encode(value)
		require.NoError(t, err)
		_, err = DecodeKeywordList(encodedValue)
		require.ErrorIs(t, err, converter.ErrUnableToDecode)
	})

	t.Run("error: non-string value in list", func(t *testing.T) {
		value := []any{"foo", 123}
		encodedValue, err := payload.Encode(value)
		require.NoError(t, err)
		_, err = DecodeKeywordList(encodedValue)
		require.ErrorIs(t, err, converter.ErrUnableToDecode)
	})

	t.Run("error: not a list", func(t *testing.T) {
		value := "foo"
		encodedValue, err := payload.Encode(value)
		require.NoError(t, err)
		_, err = DecodeKeywordList(encodedValue)
		require.ErrorIs(t, err, converter.ErrUnableToDecode)
	})

	t.Run("error: list of lists", func(t *testing.T) {
		value := [][]string{{"foo", "bar"}, {"asdf", "qwer"}}
		encodedValue, err := payload.Encode(value)
		require.NoError(t, err)
		_, err = DecodeKeywordList(encodedValue)
		require.ErrorIs(t, err, converter.ErrUnableToDecode)
	})
}

func Test_EncodeValue(t *testing.T) {
	testCases := []struct {
		name             string
		value            any
		valueType        enumspb.IndexedValueType
		expectedData     []byte
		expectedEncoding string
	}{
		{
			name:             "nil",
			value:            nil,
			valueType:        enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			expectedData:     nil,
			expectedEncoding: "binary/null",
		},
		{
			name:             "bool",
			value:            true,
			valueType:        enumspb.INDEXED_VALUE_TYPE_BOOL,
			expectedData:     []byte("true"),
			expectedEncoding: "json/plain",
		},
		{
			name:             "datetime",
			value:            time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC),
			valueType:        enumspb.INDEXED_VALUE_TYPE_DATETIME,
			expectedData:     []byte(`"2026-01-02T03:04:05.000000006Z"`),
			expectedEncoding: "json/plain",
		},
		{
			name:             "double",
			value:            1.23,
			valueType:        enumspb.INDEXED_VALUE_TYPE_DOUBLE,
			expectedData:     []byte("1.23"),
			expectedEncoding: "json/plain",
		},
		{
			name:             "int",
			value:            int64(123),
			valueType:        enumspb.INDEXED_VALUE_TYPE_INT,
			expectedData:     []byte("123"),
			expectedEncoding: "json/plain",
		},
		{
			name:             "keyword",
			value:            "foo",
			valueType:        enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			expectedData:     []byte(`"foo"`),
			expectedEncoding: "json/plain",
		},
		{
			name:             "keyword list",
			value:            []string{"foo", "bar"},
			valueType:        enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
			expectedData:     []byte(`["foo","bar"]`),
			expectedEncoding: "json/plain",
		},
		{
			name:             "text",
			value:            "foo bar",
			valueType:        enumspb.INDEXED_VALUE_TYPE_TEXT,
			expectedData:     []byte(`"foo bar"`),
			expectedEncoding: "json/plain",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			encodedValue, err := EncodeValue(tc.value, tc.valueType)
			require.NoError(t, err)
			require.Equal(t, tc.expectedData, encodedValue.Data)
			require.Equal(t, tc.expectedEncoding, string(encodedValue.Metadata[converter.MetadataEncoding]))
			require.Equal(t, tc.valueType.String(), string(encodedValue.Metadata[MetadataType]))
		})
	}

	t.Run("error", func(t *testing.T) {
		_, err := EncodeValue(math.Inf(1), enumspb.INDEXED_VALUE_TYPE_DOUBLE)
		require.Error(t, err)
	})
}

func Test_MustEncodeValue(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		_ = MustEncodeValue("foo", enumspb.INDEXED_VALUE_TYPE_KEYWORD)
	})

	t.Run("panic", func(t *testing.T) {
		defer func() {
			err := recover()
			require.NotNil(t, err)
		}()
		_ = MustEncodeValue(math.Inf(1), enumspb.INDEXED_VALUE_TYPE_DOUBLE)
		require.Fail(t, "MustEncodeValue did not panic with invalid value")
	})
}

func Test_ValidateStrings(t *testing.T) {
	_, err := validateStrings("anything here", errors.New("test error"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "test error")

	_, err = validateStrings("\x87\x01", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "is not a valid UTF-8 string")

	value, err := validateStrings("anything here", nil)
	assert.NoError(t, err)
	assert.Equal(t, "anything here", value)

	_, err = validateStrings([]string{"abc", "\x87\x01"}, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "is not a valid UTF-8 string")
}
