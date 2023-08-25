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
	"bytes"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/server/common/util"
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

// MergeMapOfPayload returns a new map resulting from merging map `src` into `dst`.
// If a key in `src` already exists in `dst`, then the value in `src` replaces
// the value in `dst`.
// If a key in `src` has nil or empty slice payload value, then it deletes
// the key from `dst` if it exists.
// For example:
//
//	dst := map[string]*commonpb.Payload{
//		"key1": EncodeString("value1"),
//		"key2": EncodeString("value2"),
//	}
//	src := map[string]*commonpb.Payload{
//		"key1": EncodeString("newValue1"),
//		"key2": nilPayload,
//	}
//	res := MergeMapOfPayload(dst, src)
//
// The resulting map `res` is:
//
//	map[string]*commonpb.Payload{
//		"key1": EncodeString("newValue1"),
//	}
func MergeMapOfPayload(
	dst map[string]*commonpb.Payload,
	src map[string]*commonpb.Payload,
) map[string]*commonpb.Payload {
	if src == nil {
		return maps.Clone(dst)
	}
	res := util.CloneMapNonNil(dst)
	for k, v := range src {
		if isEqual(v, nilPayload) || isEqual(v, emptySlicePayload) {
			delete(res, k)
		} else {
			res[k] = v
		}
	}
	return res
}

// isEqual returns true if both have the same encoding and data.
// It does not take additional metadata into consideration.
// Note that data equality it's not the same as semantic equality, ie.,
// `[]` and `[ ]` are semantically the same, but different not data-wise.
// Only use if you know that the data is encoded the same way.
func isEqual(a, b *commonpb.Payload) bool {
	aEnc := a.GetMetadata()[converter.MetadataEncoding]
	bEnc := a.GetMetadata()[converter.MetadataEncoding]
	return bytes.Equal(aEnc, bEnc) && bytes.Equal(a.GetData(), b.GetData())
}
