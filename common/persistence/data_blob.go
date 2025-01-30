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

package persistence

import (
	"encoding/json"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence/serialization"
	"google.golang.org/protobuf/proto"
)

type ProtoBlob[T proto.Message] struct {
	*commonpb.DataBlob
}

// NewDataBlob returns a new DataBlob
// TODO: return an UnknowEncodingType error with the actual type string when encodingTypeStr is invalid
func NewDataBlob(data []byte, encodingTypeStr string) *commonpb.DataBlob {
	if len(data) == 0 {
		return nil
	}

	encodingType, err := enumspb.EncodingTypeFromString(encodingTypeStr)
	if err != nil {
		// encodingTypeStr not valid, an error will be returned on deserialization
		encodingType = enumspb.ENCODING_TYPE_UNSPECIFIED
	}

	return &commonpb.DataBlob{
		Data:         data,
		EncodingType: encodingType,
	}
}

func NewProtoBlob[T proto.Message](blob *commonpb.DataBlob) *ProtoBlob[T] {
	return &ProtoBlob[T]{DataBlob: blob}
}

func (d *ProtoBlob[T]) MarshalJSON() ([]byte, error) {
	var zero proto.Message
	if err := serialization.ProtoDecodeBlob(d.DataBlob, zero); err != nil {
		return nil, err
	}
	return json.Marshal(d)
}
