package searchattribute

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
)

func Test_Encode_Success(t *testing.T) {
	r := require.New(t)

	sa, err := Encode(map[string]any{
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
	r.Len(sa.IndexedFields, 6)
	r.Equal(`"val1"`, string(sa.IndexedFields["key1"].GetData()))
	r.Equal("Text", string(sa.IndexedFields["key1"].GetMetadata()["type"]))
	r.Equal("2", string(sa.IndexedFields["key2"].GetData()))
	r.Equal("Int", string(sa.IndexedFields["key2"].GetMetadata()["type"]))
	r.Equal("true", string(sa.IndexedFields["key3"].GetData()))
	r.Equal("Bool", string(sa.IndexedFields["key3"].GetMetadata()["type"]))
	r.Empty(string(sa.IndexedFields["key4"].GetData()))
	r.Equal("Double", string(sa.IndexedFields["key4"].GetMetadata()["type"]))
	r.Equal("binary/null", string(sa.IndexedFields["key4"].GetMetadata()["encoding"]))
	r.Equal(`["val2","val3"]`, string(sa.IndexedFields["key5"].GetData()))
	r.Equal("Keyword", string(sa.IndexedFields["key5"].GetMetadata()["type"]))
	r.Equal("json/plain", string(sa.IndexedFields["key5"].GetMetadata()["encoding"]))
	r.Equal("[]", string(sa.IndexedFields["key6"].GetData()))
	r.Equal("Keyword", string(sa.IndexedFields["key6"].GetMetadata()["type"]))
	r.Equal("json/plain", string(sa.IndexedFields["key6"].GetMetadata()["encoding"]))
}
func Test_Encode_NilMap(t *testing.T) {
	r := require.New(t)

	sa, err := Encode(map[string]any{
		"key1": "val1",
		"key2": 2,
		"key3": true,
		"key4": nil,
		"key5": []string{"val2", "val3"},
		"key6": []string{},
	}, nil)

	r.NoError(err)
	r.Len(sa.IndexedFields, 6)
	r.Equal(`"val1"`, string(sa.IndexedFields["key1"].GetData()))
	r.Equal("2", string(sa.IndexedFields["key2"].GetData()))
	r.Equal("true", string(sa.IndexedFields["key3"].GetData()))
	r.Empty(string(sa.IndexedFields["key4"].GetData()))
	r.Equal("binary/null", string(sa.IndexedFields["key4"].GetMetadata()["encoding"]))
	r.Equal(`["val2","val3"]`, string(sa.IndexedFields["key5"].GetData()))
	r.Equal("json/plain", string(sa.IndexedFields["key5"].GetMetadata()["encoding"]))
	r.Equal("[]", string(sa.IndexedFields["key6"].GetData()))
	r.Equal("json/plain", string(sa.IndexedFields["key6"].GetMetadata()["encoding"]))
}

func Test_Encode_SkipsUnknownSearchAttributes(t *testing.T) {
	r := require.New(t)
	sa, err := Encode(map[string]any{
		"key1": "val1",
		"key2": 2,
		"key3": true,
	}, &NameTypeMap{customSearchAttributes: map[string]enumspb.IndexedValueType{
		"key1": enumspb.INDEXED_VALUE_TYPE_TEXT,
		"key3": enumspb.INDEXED_VALUE_TYPE_BOOL,
	}})

	// key2 is unknown and should be silently skipped.
	r.NoError(err)
	r.Len(sa.IndexedFields, 2)
	r.Equal(`"val1"`, string(sa.IndexedFields["key1"].GetData()))
	r.Equal("Text", string(sa.IndexedFields["key1"].GetMetadata()["type"]))
	r.NotContains(sa.IndexedFields, "key2")
	r.Equal("true", string(sa.IndexedFields["key3"].GetData()))
	r.Equal("Bool", string(sa.IndexedFields["key3"].GetMetadata()["type"]))
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
	sa, err := Encode(map[string]any{
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

	delete(sa.IndexedFields["key1"].Metadata, "type")
	delete(sa.IndexedFields["key2"].Metadata, "type")
	delete(sa.IndexedFields["key3"].Metadata, "type")
	delete(sa.IndexedFields["key4"].Metadata, "type")
	delete(sa.IndexedFields["key5"].Metadata, "type")
	delete(sa.IndexedFields["key6"].Metadata, "type")

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
	sa, err := Encode(map[string]any{
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
	r.Len(sa.IndexedFields, 6)
	r.Equal("val1", vals["key1"])
	r.Equal(int64(2), vals["key2"])
	r.Equal(true, vals["key3"])
	r.Nil(vals["key4"])
	r.Equal([]string{"val2", "val3"}, vals["key5"])
	r.Nil(vals["key6"])
}

func Test_Decode_SkipsUnknownSearchAttributes(t *testing.T) {
	r := require.New(t)

	typeMap := &NameTypeMap{customSearchAttributes: map[string]enumspb.IndexedValueType{
		"key1": enumspb.INDEXED_VALUE_TYPE_TEXT,
		"key2": enumspb.INDEXED_VALUE_TYPE_INT,
		"key3": enumspb.INDEXED_VALUE_TYPE_BOOL,
	}}
	sa, err := Encode(map[string]any{
		"key1": "val1",
		"key2": 2,
		"key3": true,
	}, typeMap)
	r.NoError(err)

	// Decode with a typeMap that doesn't include key2: key2 should be silently skipped.
	vals, err := Decode(
		sa,
		&NameTypeMap{customSearchAttributes: map[string]enumspb.IndexedValueType{
			"key1": enumspb.INDEXED_VALUE_TYPE_TEXT,
			"key3": enumspb.INDEXED_VALUE_TYPE_BOOL,
		}},
		true,
	)
	r.NoError(err)
	r.Len(vals, 2)
	r.Equal("val1", vals["key1"])
	r.NotContains(vals, "key2")
	r.Equal(true, vals["key3"])
}

func Test_Decode_Error(t *testing.T) {
	r := require.New(t)

	typeMap := &NameTypeMap{customSearchAttributes: map[string]enumspb.IndexedValueType{
		"key1": enumspb.INDEXED_VALUE_TYPE_TEXT,
		"key2": enumspb.INDEXED_VALUE_TYPE_INT,
		"key3": enumspb.INDEXED_VALUE_TYPE_BOOL,
	}}
	sa, err := Encode(map[string]any{
		"key1": "val1",
		"key2": 2,
		"key3": true,
	}, typeMap)
	r.NoError(err)

	delete(sa.IndexedFields["key1"].Metadata, "type")
	delete(sa.IndexedFields["key2"].Metadata, "type")
	delete(sa.IndexedFields["key3"].Metadata, "type")

	vals, err := Decode(sa, nil, true)
	r.Error(err)
	r.ErrorIs(err, ErrInvalidType)
	r.Len(vals, 3)
	r.Nil(vals["key1"])
	r.Nil(vals["key2"])
	r.Nil(vals["key3"])
}
