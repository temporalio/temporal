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

package testcore

import (
	"bytes"
	"encoding/gob"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
)

// TestDataConverter implements encoded.DataConverter using gob
type TestDataConverter struct {
	NumOfCallToPayloads   int // for testing to know testDataConverter is called as expected
	NumOfCallFromPayloads int
}

func NewTestDataConverter() converter.DataConverter {
	return &TestDataConverter{}
}

func (tdc *TestDataConverter) ToPayloads(values ...interface{}) (*commonpb.Payloads, error) {
	tdc.NumOfCallToPayloads++
	result := &commonpb.Payloads{}
	for i, value := range values {
		p, err := tdc.ToPayload(value)
		if err != nil {
			return nil, fmt.Errorf(
				"args[%d], %T: %w", i, value, err)
		}
		result.Payloads = append(result.Payloads, p)
	}
	return result, nil
}

func (tdc *TestDataConverter) FromPayloads(payloads *commonpb.Payloads, valuePtrs ...interface{}) error {
	tdc.NumOfCallFromPayloads++
	for i, p := range payloads.GetPayloads() {
		err := tdc.FromPayload(p, valuePtrs[i])
		if err != nil {
			return fmt.Errorf("args[%d]: %w", i, err)
		}
	}
	return nil
}

func (tdc *TestDataConverter) ToPayload(value interface{}) (*commonpb.Payload, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(value); err != nil {
		return nil, err
	}
	p := &commonpb.Payload{
		Metadata: map[string][]byte{
			"encoding": []byte("gob"),
		},
		Data: buf.Bytes(),
	}
	return p, nil
}

func (tdc *TestDataConverter) FromPayload(payload *commonpb.Payload, valuePtr interface{}) error {
	encoding, ok := payload.GetMetadata()["encoding"]
	if !ok {
		return ErrEncodingIsNotSet
	}

	e := string(encoding)
	if e != "gob" {
		return ErrEncodingIsNotSupported
	}

	return decodeGob(payload, valuePtr)
}

func (tdc *TestDataConverter) ToStrings(payloads *commonpb.Payloads) []string {
	var result []string
	for _, p := range payloads.GetPayloads() {
		result = append(result, tdc.ToString(p))
	}

	return result
}

func (tdc *TestDataConverter) ToString(payload *commonpb.Payload) string {
	encoding, ok := payload.GetMetadata()["encoding"]
	if !ok {
		return ErrEncodingIsNotSet.Error()
	}

	e := string(encoding)
	if e != "gob" {
		return ErrEncodingIsNotSupported.Error()
	}

	var value interface{}
	err := decodeGob(payload, &value)
	if err != nil {
		return err.Error()
	}

	return fmt.Sprintf("%+v", value)
}

func decodeGob(payload *commonpb.Payload, valuePtr interface{}) error {
	dec := gob.NewDecoder(bytes.NewBuffer(payload.GetData()))
	return dec.Decode(valuePtr)
}
