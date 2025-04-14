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

package tdbg

import (
	"fmt"
	"io"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type (
	// TaskBlobEncoder takes a blob for a given task category and encodes it to a human-readable format.
	// Here's a breakdown of the relationship between all the related types needed to implement a custom encoder:
	// - NewCliApp accepts a list of Option objects.
	// - Option objects modify Params.
	// - Params contain a TaskBlobEncoder.
	// - TaskBlobEncoder is implemented by ProtoTaskBlobEncoder.
	// - ProtoTaskBlobEncoder uses protojson to marshal [proto.Message] objects from a TaskBlobProtoDeserializer.
	// - TaskBlobProtoDeserializer is implemented by the stock PredefinedTaskBlobDeserializer.
	// - PredefinedTaskBlobDeserializer deserializes [commonpb.DataBlob] objects into [proto.Message] objects.
	TaskBlobEncoder interface {
		Encode(writer io.Writer, taskCategoryID int, blob *commonpb.DataBlob) error
	}
	// TaskBlobEncoderFn implements TaskBlobEncoder by calling a function.
	TaskBlobEncoderFn func(writer io.Writer, taskCategoryID int, blob *commonpb.DataBlob) error
	// TaskBlobProtoDeserializer is used to deserialize task blobs into proto messages. This makes it easier to create
	// an encoder if your tasks are all backed by protos. We separate this from the encoder because we don't want the
	// encoder to be tied to protos as the wire format.
	TaskBlobProtoDeserializer interface {
		Deserialize(taskCategoryID int, blob *commonpb.DataBlob) (proto.Message, error)
	}
	// ProtoTaskBlobEncoder is a TaskBlobEncoder that uses a TaskBlobProtoDeserializer to deserialize the blob into a
	// proto message, and then uses protojson to marshal the proto message into a human-readable format.
	ProtoTaskBlobEncoder struct {
		deserializer TaskBlobProtoDeserializer
	}
	// PredefinedTaskBlobDeserializer is a TaskBlobProtoDeserializer that deserializes task blobs into the predefined
	// task categories that are used by Temporal. If your server has custom categories, you'll want to build something
	// on top of this.
	PredefinedTaskBlobDeserializer struct{}
)

var (
	jsonpbMarshaler = protojson.MarshalOptions{
		UseEnumNumbers: false, // It's ok to use strings because this is for human consumption.
		Indent:         "  ",  // Indent with two spaces to pretty-print.
		UseProtoNames:  true,  // The proto field names are more JSON-esque than the Go field names.
	}
)

// NewProtoTaskBlobEncoder returns a TaskBlobEncoder that uses a TaskBlobProtoDeserializer to deserialize the blob.
func NewProtoTaskBlobEncoder(deserializer TaskBlobProtoDeserializer) *ProtoTaskBlobEncoder {
	return &ProtoTaskBlobEncoder{
		deserializer: deserializer,
	}
}

// NewPredefinedTaskBlobDeserializer returns a TaskBlobProtoDeserializer that works for the stock task categories of the
// server. You need to extend this if you have custom task categories.
func NewPredefinedTaskBlobDeserializer() PredefinedTaskBlobDeserializer {
	return PredefinedTaskBlobDeserializer{}
}

// Deserialize a task blob from one of the server's predefined task categories into a proto message.
func (d PredefinedTaskBlobDeserializer) Deserialize(categoryID int, blob *commonpb.DataBlob) (proto.Message, error) {
	switch categoryID {
	case tasks.CategoryIDTransfer:
		return serialization.TransferTaskInfoFromBlob(blob.Data, blob.EncodingType.String())
	case tasks.CategoryIDTimer:
		return serialization.TimerTaskInfoFromBlob(blob.Data, blob.EncodingType.String())
	case tasks.CategoryIDVisibility:
		return serialization.VisibilityTaskInfoFromBlob(blob.Data, blob.EncodingType.String())
	case tasks.CategoryIDReplication:
		return serialization.ReplicationTaskInfoFromBlob(blob.Data, blob.EncodingType.String())
	case tasks.CategoryIDArchival:
		return serialization.ArchivalTaskInfoFromBlob(blob.Data, blob.EncodingType.String())
	case tasks.CategoryIDOutbound:
		return serialization.OutboundTaskInfoFromBlob(blob.Data, blob.EncodingType.String())
	default:
		return nil, fmt.Errorf("unsupported task category %v", categoryID)
	}
}

// Encode a blob for a given task category to a human-readable format by deserializing the blob into a proto message and
// then pretty-printing it using protojson.
func (e *ProtoTaskBlobEncoder) Encode(writer io.Writer, categoryID int, blob *commonpb.DataBlob) error {
	message, err := e.deserializer.Deserialize(categoryID, blob)
	if err != nil {
		return fmt.Errorf("failed to deserialize task blob: %w", err)
	}
	bs, err := jsonpbMarshaler.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal task blob: %w", err)
	}
	_, err = writer.Write(bs)
	if err != nil {
		return fmt.Errorf("failed to write marshalled task blob: %w", err)
	}
	return nil
}

// Encode the task by calling the function.
func (e TaskBlobEncoderFn) Encode(writer io.Writer, taskCategoryID int, blob *commonpb.DataBlob) error {
	return e(writer, taskCategoryID, blob)
}
