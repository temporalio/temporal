package payload

import (
	"testing"

	"github.com/stretchr/testify/assert"
	commonpb "go.temporal.io/api/common/v1"
)

type testStruct struct {
	Int    int
	String string
	Bytes  []byte
}

func TestToString(t *testing.T) {
	s := assert.New(t)
	var result string

	p := EncodeString("str")
	result = ToString(p)
	s.Equal(`"str"`, result)

	p, err := Encode(10)
	s.NoError(err)
	result = ToString(p)
	s.Equal("10", result)

	p, err = Encode([]byte{41, 42, 43})
	s.NoError(err)
	result = ToString(p)
	s.Equal("KSor", result)

	p, err = Encode(&testStruct{
		Int:    10,
		String: "str",
		Bytes:  []byte{51, 52, 53},
	})
	s.NoError(err)
	result = ToString(p)
	s.Equal(`{"Int":10,"String":"str","Bytes":"MzQ1"}`, result)

	p, err = Encode(nil)
	s.NoError(err)
	result = ToString(p)
	s.Equal("nil", result)

	result = ToString(nil)
	s.Equal("", result)
}

func TestEncodeDecode(t *testing.T) {
	s := assert.New(t)

	var p *commonpb.Payload
	var err error

	var decodedValueStr string
	valueStr := "asdf"
	p, err = Encode(valueStr)
	s.NoError(err)
	err = Decode(p, &decodedValueStr)
	s.NoError(err)
	s.Equal(valueStr, decodedValueStr)

	var decodedValueInt int64
	valueInt := int64(1234)
	p, err = Encode(valueInt)
	s.NoError(err)
	err = Decode(p, &decodedValueInt)
	s.NoError(err)
	s.Equal(valueInt, decodedValueInt)

	var decodedValueBool bool
	valueBool := true
	p, err = Encode(valueBool)
	s.NoError(err)
	err = Decode(p, &decodedValueBool)
	s.NoError(err)
	s.Equal(valueBool, decodedValueBool)

	var decodedValueAny any
	var valueAny any = 123.45
	p, err = Encode(valueAny)
	s.NoError(err)
	err = Decode(p, &decodedValueAny)
	s.NoError(err)
	s.Equal(valueAny, decodedValueAny)
}

func TestMergeMapOfPayload(t *testing.T) {
	s := assert.New(t)

	var currentMap map[string]*commonpb.Payload
	var newMap map[string]*commonpb.Payload
	resultMap := MergeMapOfPayload(currentMap, newMap)
	s.Equal(newMap, resultMap)

	newMap = make(map[string]*commonpb.Payload)
	resultMap = MergeMapOfPayload(currentMap, newMap)
	s.Equal(newMap, resultMap)

	newMap = map[string]*commonpb.Payload{"key": EncodeString("val")}
	resultMap = MergeMapOfPayload(currentMap, newMap)
	s.Equal(newMap, resultMap)

	newMap = map[string]*commonpb.Payload{
		"key":        EncodeString("val"),
		"nil":        nilPayload,
		"emptyArray": emptySlicePayload,
	}
	resultMap = MergeMapOfPayload(currentMap, newMap)
	s.Equal(map[string]*commonpb.Payload{"key": EncodeString("val")}, resultMap)

	currentMap = map[string]*commonpb.Payload{"number": EncodeString("1")}
	resultMap = MergeMapOfPayload(currentMap, newMap)
	s.Equal(
		map[string]*commonpb.Payload{"number": EncodeString("1"), "key": EncodeString("val")},
		resultMap,
	)

	newValue, _ := Encode(nil)
	newMap = map[string]*commonpb.Payload{"number": newValue}
	resultMap = MergeMapOfPayload(currentMap, newMap)
	s.Equal(0, len(resultMap))

	newValue, _ = Encode([]int{})
	newValue.Metadata["key"] = []byte("foo")
	newMap = map[string]*commonpb.Payload{"number": newValue}
	resultMap = MergeMapOfPayload(currentMap, newMap)
	s.Equal(0, len(resultMap))
}

func TestIsEqual(t *testing.T) {
	s := assert.New(t)

	a, _ := Encode(nil)
	b, _ := Encode(nil)
	s.True(isEqual(a, b))

	a, _ = Encode([]string{})
	b, _ = Encode([]string{})
	s.True(isEqual(a, b))

	a.Metadata["key"] = []byte("foo")
	b.Metadata["key"] = []byte("bar")
	s.True(isEqual(a, b))

	a, _ = Encode(nil)
	b, _ = Encode([]string{})
	s.False(isEqual(a, b))

	a, _ = Encode([]string{})
	b, _ = Encode("foo")
	s.False(isEqual(a, b))
}

func TestFilterNilSearchAttributes(t *testing.T) {
	s := assert.New(t)

	// nil input returns nil
	result := FilterNilSearchAttributes(nil)
	s.Nil(result)

	// empty SearchAttributes returns itself
	emptySA := &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{}}
	result = FilterNilSearchAttributes(emptySA)
	s.Equal(emptySA, result)

	// SearchAttributes with only valid values returns filtered copy
	validPayload := EncodeString("value")
	saNonNil := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"key1": validPayload,
		},
	}
	result = FilterNilSearchAttributes(saNonNil)
	s.NotNil(result)
	s.Len(result.IndexedFields, 1)
	s.Equal(validPayload, result.IndexedFields["key1"])

	// SearchAttributes with nil values filters them out
	nilPayloadVal, _ := Encode(nil)
	saMixed := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"valid":  validPayload,
			"nilVal": nilPayloadVal,
		},
	}
	result = FilterNilSearchAttributes(saMixed)
	s.NotNil(result)
	s.Len(result.IndexedFields, 1)
	s.Equal(validPayload, result.IndexedFields["valid"])
	s.Nil(result.IndexedFields["nilVal"])

	// SearchAttributes with empty slice values filters them out
	emptySlicePayloadVal, _ := Encode([]string{})
	saEmptySlice := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"valid":      validPayload,
			"emptySlice": emptySlicePayloadVal,
		},
	}
	result = FilterNilSearchAttributes(saEmptySlice)
	s.NotNil(result)
	s.Len(result.IndexedFields, 1)
	s.Equal(validPayload, result.IndexedFields["valid"])

	// SearchAttributes with all nil/empty values returns nil
	saAllNil := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"nil1": nilPayloadVal,
			"nil2": emptySlicePayloadVal,
		},
	}
	result = FilterNilSearchAttributes(saAllNil)
	s.Nil(result)
}
