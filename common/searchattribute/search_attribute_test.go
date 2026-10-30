package searchattribute

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/searchattribute/sadefs"
)

func Test_ApplyTypeMap_Success(t *testing.T) {
	assert := assert.New(t)

	payloadInt, err := payload.Encode(123)
	assert.NoError(err)

	sa := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"key1": payload.EncodeString("str"),
			"key2": payload.EncodeString("keyword"),
			"key3": payloadInt,
		},
	}

	validSearchAttributes := NameTypeMap{customSearchAttributes: map[string]enumspb.IndexedValueType{
		"key1": enumspb.INDEXED_VALUE_TYPE_TEXT,
		"key2": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"key3": enumspb.INDEXED_VALUE_TYPE_INT,
		"key4": enumspb.INDEXED_VALUE_TYPE_DOUBLE,
	}}

	ApplyTypeMap(sa, validSearchAttributes)
	assert.Equal("Text", string(sa.GetIndexedFields()["key1"].Metadata["type"]))
	assert.Equal("Keyword", string(sa.GetIndexedFields()["key2"].Metadata["type"]))
	assert.Equal("Int", string(sa.GetIndexedFields()["key3"].Metadata["type"]))
}

func Test_ApplyTypeMap_Skip(t *testing.T) {
	assert := assert.New(t)

	payloadInt, err := payload.Encode(123)
	assert.NoError(err)
	payloadInt.Metadata["type"] = []byte("String")

	sa := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"UnknownKey": payload.EncodeString("str"),
			"key4":       payload.EncodeString("invalid IndexValueType"),
			"key3":       payloadInt, // Another type already set
		},
	}

	validSearchAttributes := NameTypeMap{customSearchAttributes: map[string]enumspb.IndexedValueType{
		"key1": enumspb.INDEXED_VALUE_TYPE_TEXT,
		"key2": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"key3": enumspb.INDEXED_VALUE_TYPE_INT,
	}}

	ApplyTypeMap(sa, validSearchAttributes)
	assert.Nil(sa.GetIndexedFields()["UnknownKey"].Metadata["type"])
	assert.Equal("String", string(sa.GetIndexedFields()["key3"].Metadata["type"]))

	validSearchAttributes = NameTypeMap{customSearchAttributes: map[string]enumspb.IndexedValueType{
		"key4": enumspb.IndexedValueType(100),
	}}

	assert.Panics(func() {
		ApplyTypeMap(sa, validSearchAttributes)
	})
}

func Test_ApplyTypeMapWithMapper(t *testing.T) {
	typeMap := NameTypeMap{customSearchAttributes: map[string]enumspb.IndexedValueType{
		"Keyword01": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"Int01":     enumspb.INDEXED_VALUE_TYPE_INT,
	}}

	payloadWithType := payload.EncodeString("str")
	payloadWithType.Metadata[MetadataType] = []byte("Text")
	sa := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"AliasForKeyword01":          payload.EncodeString("keyword"),
			"AliasForInt01":              payloadWithType, // Type already set.
			sadefs.TemporalChangeVersion: payload.EncodeString("change"),
			"wrong_alias":                payload.EncodeString("unknown"),
			"AliasForUnknown":            payload.EncodeString("unknown"),
		},
	}

	ApplyTypeMapWithMapper(sa, typeMap, &TestMapper{}, "test-namespace")
	require.Equal(t, "Keyword", string(sa.GetIndexedFields()["AliasForKeyword01"].GetMetadata()[MetadataType]))
	require.Equal(t, "Text", string(sa.GetIndexedFields()["AliasForInt01"].GetMetadata()[MetadataType]))
	require.Equal(t, "KeywordList", string(sa.GetIndexedFields()[sadefs.TemporalChangeVersion].GetMetadata()[MetadataType]))
	require.NotContains(t, sa.GetIndexedFields()["wrong_alias"].GetMetadata(), MetadataType)
	require.NotContains(t, sa.GetIndexedFields()["AliasForUnknown"].GetMetadata(), MetadataType)

	// Without a mapper, names are used as field names.
	sa = &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"Keyword01":                  payload.EncodeString("keyword"),
			sadefs.TemporalChangeVersion: payload.EncodeString("change"),
		},
	}
	ApplyTypeMapWithMapper(sa, typeMap, nil, "test-namespace")
	require.Equal(t, "Keyword", string(sa.GetIndexedFields()["Keyword01"].GetMetadata()[MetadataType]))
	require.Equal(t, "KeywordList", string(sa.GetIndexedFields()[sadefs.TemporalChangeVersion].GetMetadata()[MetadataType]))
}
