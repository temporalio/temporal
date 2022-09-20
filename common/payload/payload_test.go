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
	assert := assert.New(t)
	var result string

	p := EncodeString("str")
	result = ToString(p)
	assert.Equal(`"str"`, result)

	p, err := Encode(10)
	assert.NoError(err)
	result = ToString(p)
	assert.Equal("10", result)

	p, err = Encode([]byte{41, 42, 43})
	assert.NoError(err)
	result = ToString(p)
	assert.Equal("KSor", result)

	p, err = Encode(&testStruct{
		Int:    10,
		String: "str",
		Bytes:  []byte{51, 52, 53},
	})
	assert.NoError(err)
	result = ToString(p)
	assert.Equal(`{"Int":10,"String":"str","Bytes":"MzQ1"}`, result)

	p, err = Encode(nil)
	assert.NoError(err)
	result = ToString(p)
	assert.Equal("nil", result)

	result = ToString(nil)
	assert.Equal("", result)
}

func TestMergeMapOfPayload(t *testing.T) {
	assert := assert.New(t)

	var currentMap map[string]*commonpb.Payload
	var newMap map[string]*commonpb.Payload
	resultMap := MergeMapOfPayload(currentMap, newMap)
	assert.Equal(newMap, resultMap)

	newMap = make(map[string]*commonpb.Payload)
	resultMap = MergeMapOfPayload(currentMap, newMap)
	assert.Equal(newMap, resultMap)

	newMap = map[string]*commonpb.Payload{"key": EncodeString("val")}
	resultMap = MergeMapOfPayload(currentMap, newMap)
	assert.Equal(newMap, resultMap)

	newMap = map[string]*commonpb.Payload{
		"key":        EncodeString("val"),
		"nil":        nilPayload,
		"emptyArray": emptySlicePayload,
	}
	resultMap = MergeMapOfPayload(currentMap, newMap)
	assert.Equal(map[string]*commonpb.Payload{"key": EncodeString("val")}, resultMap)

	currentMap = map[string]*commonpb.Payload{"number": EncodeString("1")}
	resultMap = MergeMapOfPayload(currentMap, newMap)
	assert.Equal(
		map[string]*commonpb.Payload{"number": EncodeString("1"), "key": EncodeString("val")},
		resultMap,
	)

	newValue, _ := Encode(nil)
	newMap = map[string]*commonpb.Payload{"number": newValue}
	resultMap = MergeMapOfPayload(currentMap, newMap)
	assert.Equal(0, len(resultMap))

	newValue, _ = Encode([]int{})
	newMap = map[string]*commonpb.Payload{"number": newValue}
	resultMap = MergeMapOfPayload(currentMap, newMap)
	assert.Equal(0, len(resultMap))
}
