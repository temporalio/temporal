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
	"mime"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
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

	if !isStandardNexusContent(content) {
		for k, v := range content.Header {
			payload.Metadata[k] = []byte(v)
		}
		payload.Metadata["encoding"] = []byte("unknown/nexus-content")
		return nil
	}

	contentType := content.Header.Get("type")
	if contentType == "" {
		payload.Metadata["encoding"] = []byte("binary/null")
		return nil
	}

	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		return err
	}

	switch mediaType {
	case "application/x-temporal-payload":
		err := payload.Unmarshal(content.Data)
		if err != nil {
			return err
		}
		return nil
	case "application/json":
		if params["format"] == "protobuf" {
			payload.Metadata["encoding"] = []byte("json/protobuf")
			messageType := params["message-type"]
			if messageType != "" {
				payload.Metadata["messageType"] = []byte(messageType)
			}
		} else {
			payload.Metadata["encoding"] = []byte("json/plain")
		}
	case "application/x-protobuf":
		payload.Metadata["encoding"] = []byte("binary/protobuf")
		messageType := params["message-type"]
		if messageType != "" {
			payload.Metadata["messageType"] = []byte(messageType)
		}
	case "application/octet-stream":
		payload.Metadata["encoding"] = []byte("binary/plain")
	default:
		// Should be unreachable.
		return fmt.Errorf("%w: standard content detection failed for %q", errSerializer, mediaType)
	}
	return nil
}

func isStandardNexusContent(content *nexus.Content) bool {
	h := content.Header
	// We assume that encoding is handled by the transport layer and the content is decoded.
	delete(h, "encoding")
	// Length can safely be ignored.
	delete(h, "length")

	if len(h) > 1 {
		return false
	}
	contentType := h.Get("type")
	if contentType == "" {
		return len(h) == 0 && len(content.Data) == 0
	}
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		return false
	}

	switch mediaType {
	case "application/octet-stream",
		"application/x-temporal-payload":
		return len(params) == 0
	case "application/json":
		if params["format"] == "protobuf" {
			if params["message-type"] != "" {
				return len(params) == 2
			}
			return len(params) == 1
		}
		return len(params) == 0
	case "application/x-protobuf":
		if params["message-type"] != "" {
			return len(params) == 1
		}
		return len(params) == 0
	}
	return false
}

// Serialize implements nexus.Serializer.
func (payloadSerializer) Serialize(v any) (*nexus.Content, error) {
	payload, ok := v.(*commonpb.Payload)
	if !ok {
		return nil, fmt.Errorf("%w: cannot serialize %v", errSerializer, v)
	}

	if payload == nil {
		return &nexus.Content{Header: nexus.Header{}}, nil
	}

	if !isStandardPayload(payload) {
		data, err := payload.Marshal()
		if err != nil {
			return nil, fmt.Errorf("%w: payload marshal error: %w", errSerializer, err)
		}
		return &nexus.Content{
			Header: nexus.Header{"type": "application/x-temporal-payload"},
			Data:   data,
		}, nil
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
		content.Header["type"] = fmt.Sprintf("application/json; format=protobuf")
		if messageType != "" {
			content.Header["type"] += fmt.Sprintf("; message-type=%q", messageType)
		}
	case "binary/protobuf":
		content.Header["type"] = fmt.Sprintf("application/x-protobuf")
		if messageType != "" {
			content.Header["type"] += fmt.Sprintf("; message-type=%q", messageType)
		}
	case "json/plain":
		content.Header["type"] = "application/json"
	case "binary/null":
		// type is unset
	case "binary/plain":
		content.Header["type"] = "application/octet-stream"
	default:
		// Should be unreachable.
		return nil, fmt.Errorf("%w: standard payload detection failed for encoding: %q", errSerializer, encoding)
	}

	return &content, nil
}

var _ nexus.Serializer = payloadSerializer{}

func isStandardPayload(payload *commonpb.Payload) bool {
	if payload.GetMetadata() == nil {
		return false
	}

	encoding := string(payload.Metadata["encoding"])
	switch encoding {
	case "unknown/nexus-content":
		return true
	case "json/protobuf",
		"binary/protobuf":
		if _, ok := payload.Metadata["messageType"]; ok {
			return len(payload.Metadata) == 2
		}
		return len(payload.Metadata) == 1
	case "json/plain",
		"binary/null",
		"binary/plain":
		return len(payload.Metadata) == 1
	}
	return false
}
