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

// This whole file exists because grpc-gateway's runtime.JSONPb doesn't support indentation
package frontend

import (
	"encoding/json"
	"io"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var _ runtime.Marshaler = protoJsonMarshaler{}

type protoJsonMarshaler struct {
	mOpts protojson.MarshalOptions
	uOpts protojson.UnmarshalOptions
}

type protoJsonEncoder struct {
	mOpts  protojson.MarshalOptions
	writer io.Writer
	json   *json.Encoder
}

type protoJsonDecoder struct {
	uOpts  protojson.UnmarshalOptions
	reader io.Reader
	json   *json.Decoder
}

func newProtoJsonMarshaler(indent string) protoJsonMarshaler {
	return protoJsonMarshaler{
		mOpts: protojson.MarshalOptions{
			Indent: indent,
		},
	}
}

func (p protoJsonMarshaler) Marshal(v any) ([]byte, error) {
	if m, ok := v.(proto.Message); ok {
		return p.mOpts.Marshal(m)
	}

	if p.mOpts.Indent != "" {
		return json.MarshalIndent(v, "", p.mOpts.Indent)
	}

	return json.Marshal(v)
}

func (p protoJsonMarshaler) Unmarshal(data []byte, v interface{}) error {
	if m, ok := v.(proto.Message); ok {
		return protojson.Unmarshal(data, m)
	}

	return json.Unmarshal(data, v)
}

func (p protoJsonMarshaler) NewDecoder(r io.Reader) runtime.Decoder {
	return protoJsonDecoder{
		p.uOpts,
		r,
		json.NewDecoder(r),
	}
}

func (p protoJsonMarshaler) NewEncoder(w io.Writer) runtime.Encoder {
	return protoJsonEncoder{
		p.mOpts,
		w,
		json.NewEncoder(w),
	}
}

func (p protoJsonMarshaler) ContentType(_ any) string {
	return "application/json"
}

func (d protoJsonDecoder) Decode(v any) error {
	m, ok := v.(proto.Message)
	if !ok {
		return d.json.Decode(v)
	}

	var bs json.RawMessage
	if err := d.json.Decode(&bs); err != nil {
		return err
	}

	return protojson.Unmarshal([]byte(bs), m)
}

func (e protoJsonEncoder) Encode(v any) error {
	m, ok := v.(proto.Message)
	if !ok {
		return e.json.Encode(v)
	}

	bs, err := e.mOpts.Marshal(m)
	if err != nil {
		return err
	}

	_, err = e.writer.Write(bs)
	if err != nil {
		return err
	}
	_, err = e.writer.Write([]byte{'\n'})
	return err
}
