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
	commonpb "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal/encoded"
)

var (
	dataConverter = encoded.GetDefaultDataConverter()
)

func EncodeString(str string) *commonpb.Payload {
	// Error can be safely ignored here becase string always can be converted to JSON
	payload, _ := dataConverter.ToData(str)
	return payload
}

func EncodeBytes(bytes []byte) *commonpb.Payload {
	// Error can be safely ignored here becase []byte always can be raw encoded
	payload, _ := dataConverter.ToData(bytes)
	return payload
}

func Encode(valuePtr ...interface{}) (*commonpb.Payload, error) {
	return dataConverter.ToData(valuePtr...)
}

func Decode(payload *commonpb.Payload, valuePtr ...interface{}) error {
	return dataConverter.FromData(payload, valuePtr...)
}
