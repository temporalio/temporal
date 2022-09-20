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
	"github.com/gogo/protobuf/proto"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"golang.org/x/exp/maps"
)

var (
	defaultDataConverter = converter.GetDefaultDataConverter()

	nilPayload, _        = Encode(nil)
	emptySlicePayload, _ = Encode([]string{})
)

func EncodeString(str string) *commonpb.Payload {
	// Error can be safely ignored here because string always can be converted to JSON
	p, _ := defaultDataConverter.ToPayload(str)
	return p
}

func EncodeBytes(bytes []byte) *commonpb.Payload {
	// Error can be safely ignored here because []byte always can be raw encoded
	p, _ := defaultDataConverter.ToPayload(bytes)
	return p
}

func Encode(value interface{}) (*commonpb.Payload, error) {
	return defaultDataConverter.ToPayload(value)
}

func Decode(p *commonpb.Payload, valuePtr interface{}) error {
	return defaultDataConverter.FromPayload(p, valuePtr)
}

func ToString(p *commonpb.Payload) string {
	return defaultDataConverter.ToString(p)
}

// MergeMapOfPayload returns a new map resulting from merging map m2 into m1.
// If a key in m2 already exists in m1, then the value in m2 replaces the value in m1.
// If the new payload have nil data or an empty slice data, then it deletes the key.
// For example:
//
//	m1 := map[string]*commonpb.Payload{
//		"key1": EncodeString("value1"),
//		"key2": EncodeString("value2"),
//	}
//	m2 := map[string]*commonpb.Payload{
//		"key1": EncodeString("newValue1"),
//		"key2": nilPayload,
//	}
//	m3 := MergeMapOfPayload(m1, m2)
//
// The resulting map `m3` is:
//
//	m1 := map[string]*commonpb.Payload{
//		"key1": EncodeString("newValue1"),
//	}
func MergeMapOfPayload(
	m1 map[string]*commonpb.Payload,
	m2 map[string]*commonpb.Payload,
) map[string]*commonpb.Payload {
	if m2 == nil {
		return maps.Clone(m1)
	}
	ret := maps.Clone(m1)
	if ret == nil {
		ret = make(map[string]*commonpb.Payload)
	}
	for k, v := range m2 {
		if proto.Equal(v, nilPayload) || proto.Equal(v, emptySlicePayload) {
			delete(ret, k)
		} else {
			ret[k] = v
		}
	}
	return ret
}
