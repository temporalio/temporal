// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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
