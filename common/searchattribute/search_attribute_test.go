package searchattribute

import (
	"testing"

	"github.com/stretchr/testify/assert"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/payload"
)

func Test_ApplyTypeMap_Success(t *testing.T) {
	assert := assert.New(t)

	payloadInt, err := payload.Encode(123)
	assert.NoError(err)

	sa := commonpb.SearchAttributes_builder{
		IndexedFields: map[string]*commonpb.Payload{
			"key1": payload.EncodeString("str"),
			"key2": payload.EncodeString("keyword"),
			"key3": payloadInt,
		},
	}.Build()

	validSearchAttributes := NameTypeMap{customSearchAttributes: map[string]enumspb.IndexedValueType{
		"key1": enumspb.INDEXED_VALUE_TYPE_TEXT,
		"key2": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"key3": enumspb.INDEXED_VALUE_TYPE_INT,
		"key4": enumspb.INDEXED_VALUE_TYPE_DOUBLE,
	}}

	ApplyTypeMap(sa, validSearchAttributes)
	assert.Equal("Text", string(sa.GetIndexedFields()["key1"].GetMetadata()["type"]))
	assert.Equal("Keyword", string(sa.GetIndexedFields()["key2"].GetMetadata()["type"]))
	assert.Equal("Int", string(sa.GetIndexedFields()["key3"].GetMetadata()["type"]))
}

func Test_ApplyTypeMap_Skip(t *testing.T) {
	assert := assert.New(t)

	payloadInt, err := payload.Encode(123)
	assert.NoError(err)
	payloadInt.GetMetadata()["type"] = []byte("String")

	sa := commonpb.SearchAttributes_builder{
		IndexedFields: map[string]*commonpb.Payload{
			"UnknownKey": payload.EncodeString("str"),
			"key4":       payload.EncodeString("invalid IndexValueType"),
			"key3":       payloadInt, // Another type already set
		},
	}.Build()

	validSearchAttributes := NameTypeMap{customSearchAttributes: map[string]enumspb.IndexedValueType{
		"key1": enumspb.INDEXED_VALUE_TYPE_TEXT,
		"key2": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"key3": enumspb.INDEXED_VALUE_TYPE_INT,
	}}

	ApplyTypeMap(sa, validSearchAttributes)
	assert.Nil(sa.GetIndexedFields()["UnknownKey"].GetMetadata()["type"])
	assert.Equal("String", string(sa.GetIndexedFields()["key3"].GetMetadata()["type"]))

	validSearchAttributes = NameTypeMap{customSearchAttributes: map[string]enumspb.IndexedValueType{
		"key4": enumspb.IndexedValueType(100),
	}}

	assert.Panics(func() {
		ApplyTypeMap(sa, validSearchAttributes)
	})
}
