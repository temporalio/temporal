package searchattribute

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
)

func Test_Encode_Success(t *testing.T) {
	r := require.New(t)

	sa, err := Encode(map[string]interface{}{
		"key1": "val1",
		"key2": 2,
		"key3": true,
		"key4": nil,
		"key5": []string{"val2", "val3"},
		"key6": []string{},
	}, &NameTypeMap{customSearchAttributes: map[string]enumspb.IndexedValueType{
		"key1": enumspb.INDEXED_VALUE_TYPE_TEXT,
		"key2": enumspb.INDEXED_VALUE_TYPE_INT,
		"key3": enumspb.INDEXED_VALUE_TYPE_BOOL,
		"key4": enumspb.INDEXED_VALUE_TYPE_DOUBLE,
		"key5": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"key6": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	}})

	r.NoError(err)
	r.Len(sa.GetIndexedFields(), 6)
	r.Equal(`"val1"`, string(sa.GetIndexedFields()["key1"].GetData()))
	r.Equal("Text", string(sa.GetIndexedFields()["key1"].GetMetadata()["type"]))
	r.Equal("2", string(sa.GetIndexedFields()["key2"].GetData()))
	r.Equal("Int", string(sa.GetIndexedFields()["key2"].GetMetadata()["type"]))
	r.Equal("true", string(sa.GetIndexedFields()["key3"].GetData()))
	r.Equal("Bool", string(sa.GetIndexedFields()["key3"].GetMetadata()["type"]))
	r.Empty(string(sa.GetIndexedFields()["key4"].GetData()))
	r.Equal("Double", string(sa.GetIndexedFields()["key4"].GetMetadata()["type"]))
	r.Equal("binary/null", string(sa.GetIndexedFields()["key4"].GetMetadata()["encoding"]))
	r.Equal(`["val2","val3"]`, string(sa.GetIndexedFields()["key5"].GetData()))
	r.Equal("Keyword", string(sa.GetIndexedFields()["key5"].GetMetadata()["type"]))
	r.Equal("json/plain", string(sa.GetIndexedFields()["key5"].GetMetadata()["encoding"]))
	r.Equal("[]", string(sa.GetIndexedFields()["key6"].GetData()))
	r.Equal("Keyword", string(sa.GetIndexedFields()["key6"].GetMetadata()["type"]))
	r.Equal("json/plain", string(sa.GetIndexedFields()["key6"].GetMetadata()["encoding"]))
}
func Test_Encode_NilMap(t *testing.T) {
	r := require.New(t)

	sa, err := Encode(map[string]interface{}{
		"key1": "val1",
		"key2": 2,
		"key3": true,
		"key4": nil,
		"key5": []string{"val2", "val3"},
		"key6": []string{},
	}, nil)

	r.NoError(err)
	r.Len(sa.GetIndexedFields(), 6)
	r.Equal(`"val1"`, string(sa.GetIndexedFields()["key1"].GetData()))
	r.Equal("2", string(sa.GetIndexedFields()["key2"].GetData()))
	r.Equal("true", string(sa.GetIndexedFields()["key3"].GetData()))
	r.Empty(string(sa.GetIndexedFields()["key4"].GetData()))
	r.Equal("binary/null", string(sa.GetIndexedFields()["key4"].GetMetadata()["encoding"]))
	r.Equal(`["val2","val3"]`, string(sa.GetIndexedFields()["key5"].GetData()))
	r.Equal("json/plain", string(sa.GetIndexedFields()["key5"].GetMetadata()["encoding"]))
	r.Equal("[]", string(sa.GetIndexedFields()["key6"].GetData()))
	r.Equal("json/plain", string(sa.GetIndexedFields()["key6"].GetMetadata()["encoding"]))
}

func Test_Encode_Error(t *testing.T) {
	r := require.New(t)
	sa, err := Encode(map[string]interface{}{
		"key1": "val1",
		"key2": 2,
		"key3": true,
	}, &NameTypeMap{customSearchAttributes: map[string]enumspb.IndexedValueType{
		"key1": enumspb.INDEXED_VALUE_TYPE_TEXT,
		"key4": enumspb.INDEXED_VALUE_TYPE_INT,
		"key3": enumspb.INDEXED_VALUE_TYPE_BOOL,
	}})

	r.Error(err)
	r.ErrorIs(err, ErrInvalidName)
	r.Len(sa.GetIndexedFields(), 3)
	r.Equal(`"val1"`, string(sa.GetIndexedFields()["key1"].GetData()))
	r.Equal("Text", string(sa.GetIndexedFields()["key1"].GetMetadata()["type"]))
	r.Equal("2", string(sa.GetIndexedFields()["key2"].GetData()))
	r.Equal("true", string(sa.GetIndexedFields()["key3"].GetData()))
	r.Equal("Bool", string(sa.GetIndexedFields()["key3"].GetMetadata()["type"]))
}

func Test_Decode_Success(t *testing.T) {
	r := require.New(t)

	typeMap := &NameTypeMap{customSearchAttributes: map[string]enumspb.IndexedValueType{
		"key1": enumspb.INDEXED_VALUE_TYPE_TEXT,
		"key2": enumspb.INDEXED_VALUE_TYPE_INT,
		"key3": enumspb.INDEXED_VALUE_TYPE_BOOL,
		"key4": enumspb.INDEXED_VALUE_TYPE_DOUBLE,
		"key5": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"key6": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	}}
	sa, err := Encode(map[string]interface{}{
		"key1": "val1",
		"key2": 2,
		"key3": true,
		"key4": nil,
		"key5": []string{"val2", "val3"},
		"key6": []string{},
	}, typeMap)
	r.NoError(err)

	vals, err := Decode(sa, typeMap, true)
	r.NoError(err)
	r.Len(vals, 6)
	r.Equal("val1", vals["key1"])
	r.Equal(int64(2), vals["key2"])
	r.Equal(true, vals["key3"])
	r.Nil(vals["key4"])
	r.Equal([]string{"val2", "val3"}, vals["key5"])
	r.Nil(vals["key6"])

	delete(sa.GetIndexedFields()["key1"].GetMetadata(), "type")
	delete(sa.GetIndexedFields()["key2"].GetMetadata(), "type")
	delete(sa.GetIndexedFields()["key3"].GetMetadata(), "type")
	delete(sa.GetIndexedFields()["key4"].GetMetadata(), "type")
	delete(sa.GetIndexedFields()["key5"].GetMetadata(), "type")
	delete(sa.GetIndexedFields()["key6"].GetMetadata(), "type")

	vals, err = Decode(sa, typeMap, true)
	r.NoError(err)
	r.Len(vals, 6)
	r.Equal("val1", vals["key1"])
	r.Equal(int64(2), vals["key2"])
	r.Equal(true, vals["key3"])
	r.Nil(vals["key4"])
	r.Equal([]string{"val2", "val3"}, vals["key5"])
	r.Nil(vals["key6"])
}

func Test_Decode_NilMap(t *testing.T) {
	r := require.New(t)
	typeMap := &NameTypeMap{customSearchAttributes: map[string]enumspb.IndexedValueType{
		"key1": enumspb.INDEXED_VALUE_TYPE_TEXT,
		"key2": enumspb.INDEXED_VALUE_TYPE_INT,
		"key3": enumspb.INDEXED_VALUE_TYPE_BOOL,
		"key4": enumspb.INDEXED_VALUE_TYPE_DOUBLE,
		"key5": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"key6": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	}}
	sa, err := Encode(map[string]interface{}{
		"key1": "val1",
		"key2": 2,
		"key3": true,
		"key4": nil,
		"key5": []string{"val2", "val3"},
		"key6": []string{},
	}, typeMap)
	r.NoError(err)

	vals, err := Decode(sa, nil, true)
	r.NoError(err)
	r.Len(sa.GetIndexedFields(), 6)
	r.Equal("val1", vals["key1"])
	r.Equal(int64(2), vals["key2"])
	r.Equal(true, vals["key3"])
	r.Nil(vals["key4"])
	r.Equal([]string{"val2", "val3"}, vals["key5"])
	r.Nil(vals["key6"])
}

func Test_Decode_Error(t *testing.T) {
	r := require.New(t)

	typeMap := &NameTypeMap{customSearchAttributes: map[string]enumspb.IndexedValueType{
		"key1": enumspb.INDEXED_VALUE_TYPE_TEXT,
		"key2": enumspb.INDEXED_VALUE_TYPE_INT,
		"key3": enumspb.INDEXED_VALUE_TYPE_BOOL,
	}}
	sa, err := Encode(map[string]interface{}{
		"key1": "val1",
		"key2": 2,
		"key3": true,
	}, typeMap)
	r.NoError(err)

	vals, err := Decode(
		sa,
		&NameTypeMap{customSearchAttributes: map[string]enumspb.IndexedValueType{
			"key1": enumspb.INDEXED_VALUE_TYPE_TEXT,
			"key4": enumspb.INDEXED_VALUE_TYPE_INT,
			"key3": enumspb.INDEXED_VALUE_TYPE_BOOL,
		}},
		true,
	)
	r.Error(err)
	r.ErrorIs(err, ErrInvalidName)
	r.Len(sa.GetIndexedFields(), 3)
	r.Equal("val1", vals["key1"])
	r.Equal(int64(2), vals["key2"])
	r.Equal(true, vals["key3"])

	delete(sa.GetIndexedFields()["key1"].GetMetadata(), "type")
	delete(sa.GetIndexedFields()["key2"].GetMetadata(), "type")
	delete(sa.GetIndexedFields()["key3"].GetMetadata(), "type")

	vals, err = Decode(sa, nil, true)
	r.Error(err)
	r.ErrorIs(err, ErrInvalidType)
	r.Len(vals, 3)
	r.Nil(vals["key1"])
	r.Nil(vals["key2"])
	r.Nil(vals["key3"])
}
