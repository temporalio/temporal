// The MIT License
//
// Copyright (c) 2023 Temporal Technologies Inc.  All rights reserved.
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

package nexus

import (
	"errors"
	"fmt"
	"maps"
	"mime"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/utf8validator"
)

type payloadSerializer struct{}

var errSerializer = errors.New("serializer error")

// Deserialize implements nexus.Serializer.
func (payloadSerializer) Deserialize(content *nexus.Content, v any) error {
	payloadRef, ok := v.(**commonpb.Payload)
	if !ok {
		return fmt.Errorf("%w: cannot deserialize into %v", errSerializer, v)
	}

	payload := &commonpb.Payload{}
	*payloadRef = payload
	payload.Metadata = make(map[string][]byte)
	payload.Data = content.Data

	h := maps.Clone(content.Header)
	// We assume that encoding is handled by the transport layer and the content is decoded.
	delete(h, "encoding")
	// Length can safely be ignored.
	delete(h, "length")

	if len(h) > 1 {
		setUnknownNexusContent(h, payload.Metadata)
		return nil
	}

	contentType := h.Get("type")
	if contentType == "" {
		if len(h) == 0 && len(content.Data) == 0 {
			payload.Metadata["encoding"] = []byte("binary/null")
		} else {
			setUnknownNexusContent(h, payload.Metadata)
		}
		return nil
	}

	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		setUnknownNexusContent(h, payload.Metadata)
		return nil
	}

	switch mediaType {
	case "application/x-temporal-payload":
		err := payload.Unmarshal(content.Data)
		if err == nil {
			err = utf8validator.Validate(payload, utf8validator.SourceRPCRequest)
		}
		if err != nil {
			return serialization.NewDeserializationError(enums.ENCODING_TYPE_PROTO3, err)
		}
	case "application/json":
		if len(params) == 0 {
			payload.Metadata["encoding"] = []byte("json/plain")
		} else if len(params) == 2 && params["format"] == "protobuf" && params["message-type"] != "" {
			payload.Metadata["encoding"] = []byte("json/protobuf")
			payload.Metadata["messageType"] = []byte(params["message-type"])
		} else {
			setUnknownNexusContent(h, payload.Metadata)
		}
	case "application/x-protobuf":
		if len(params) == 1 && params["message-type"] != "" {
			payload.Metadata["encoding"] = []byte("binary/protobuf")
			payload.Metadata["messageType"] = []byte(params["message-type"])
		} else {
			setUnknownNexusContent(h, payload.Metadata)
		}
	case "application/octet-stream":
		if len(params) == 0 {
			payload.Metadata["encoding"] = []byte("binary/plain")
		} else {
			setUnknownNexusContent(h, payload.Metadata)
		}
	default:
		setUnknownNexusContent(h, payload.Metadata)
	}
	return nil
}

func setUnknownNexusContent(nexusHeader nexus.Header, payloadMetadata map[string][]byte) {
	for k, v := range nexusHeader {
		payloadMetadata[k] = []byte(v)
	}
	payloadMetadata["encoding"] = []byte("unknown/nexus-content")
}

// Serialize implements nexus.Serializer.
func (payloadSerializer) Serialize(v any) (*nexus.Content, error) {
	payload, ok := v.(*commonpb.Payload)
	if !ok {
		return nil, fmt.Errorf("%w: cannot serialize %v", errSerializer, v)
	}

	if payload == nil {
		return &nexus.Content{}, nil
	}

	if payload.GetMetadata() == nil {
		return xTemporalPayload(payload)
	}

	content := nexus.Content{Header: nexus.Header{}, Data: payload.Data}
	encoding := string(payload.Metadata["encoding"])
	messageType := string(payload.Metadata["messageType"])

	switch encoding {
	case "unknown/nexus-content":
		for k, v := range payload.Metadata {
			if k != "encoding" {
				content.Header[k] = string(v)
			}
		}
	case "json/protobuf":
		if len(payload.Metadata) != 2 || messageType == "" {
			return xTemporalPayload(payload)
		}
		content.Header["type"] = fmt.Sprintf("application/json; format=protobuf; message-type=%q", messageType)
	case "binary/protobuf":
		if len(payload.Metadata) != 2 || messageType == "" {
			return xTemporalPayload(payload)
		}
		content.Header["type"] = fmt.Sprintf("application/x-protobuf; message-type=%q", messageType)
	case "json/plain":
		content.Header["type"] = "application/json"
	case "binary/null":
		if len(payload.Metadata) != 1 {
			return xTemporalPayload(payload)
		}
		// type is unset
	case "binary/plain":
		if len(payload.Metadata) != 1 {
			return xTemporalPayload(payload)
		}
		content.Header["type"] = "application/octet-stream"
	default:
		return xTemporalPayload(payload)
	}

	return &content, nil
}

func xTemporalPayload(payload *commonpb.Payload) (*nexus.Content, error) {
	if err := utf8validator.Validate(payload, utf8validator.SourceRPCResponse); err != nil {
		return nil, fmt.Errorf("%w: payload marshal error: %w", errSerializer, err)
	}
	data, err := payload.Marshal()
	if err != nil {
		return nil, fmt.Errorf("%w: payload marshal error: %w", errSerializer, err)
	}
	return &nexus.Content{
		Header: nexus.Header{"type": "application/x-temporal-payload"},
		Data:   data,
	}, nil
}

var PayloadSerializer nexus.Serializer = payloadSerializer{}
