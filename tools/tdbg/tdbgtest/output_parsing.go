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

package tdbgtest

import (
	"encoding/json"
	"io"

	"go.temporal.io/api/temporalproto"
	"go.temporal.io/server/tools/tdbg"
	"google.golang.org/protobuf/proto"
)

type (
	// DLQMessage is a parsed version of [tdbg.DLQMessage], where the payload is a deserialized [proto.Message].
	DLQMessage[T proto.Message] struct {
		MessageID int64
		ShardID   int32
		Payload   T
	}
)

// ParseDLQMessages parses a JSONL file containing serialized [tdbg.DLQMessage] objects.
func ParseDLQMessages[T proto.Message](file io.Reader, newMessage func() T) ([]DLQMessage[T], error) {
	var opts temporalproto.CustomJSONUnmarshalOptions
	decodeNext := func(decoder *json.Decoder) (DLQMessage[T], error) {
		var dlqMessage tdbg.DLQMessage
		err := decoder.Decode(&dlqMessage)
		if err != nil {
			return DLQMessage[T]{}, err
		}
		protoMessage := newMessage()
		b := dlqMessage.Payload.Bytes()
		if err = opts.Unmarshal(b, protoMessage); err != nil {
			return DLQMessage[T]{}, err
		}
		return DLQMessage[T]{
			MessageID: dlqMessage.MessageID,
			ShardID:   dlqMessage.ShardID,
			Payload:   protoMessage,
		}, nil
	}
	return ParseJSONL(file, decodeNext)
}

// ParseJSONL parses a JSONL file. We separate this out from [ParseDLQMessages] so that we can reuse it for other JSONL
// files that don't contain [tdbg.DLQMessage] objects (i.e. when [tdbg.DLQV1Service] is used).
func ParseJSONL[T any](file io.Reader, decodeNext func(decoder *json.Decoder) (T, error)) ([]T, error) {
	decoder := json.NewDecoder(file)
	var (
		messages []T
	)
	for decoder.More() {
		message, err := decodeNext(decoder)
		if err != nil {
			return nil, err
		}
		messages = append(messages, message)
	}
	return messages, nil
}
