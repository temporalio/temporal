package nexus

import (
	"errors"
	"fmt"
	"maps"
	"mime"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence/serialization"
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
	payload.SetMetadata(make(map[string][]byte))
	payload.SetData(content.Data)

	h := maps.Clone(content.Header)
	// We assume that encoding is handled by the transport layer and the content is decoded.
	delete(h, "encoding")
	// Length can safely be ignored.
	delete(h, "length")

	if len(h) > 1 {
		setUnknownNexusContent(h, payload.GetMetadata())
		return nil
	}

	contentType := h.Get("type")
	if contentType == "" {
		if len(h) == 0 && len(content.Data) == 0 {
			payload.GetMetadata()["encoding"] = []byte("binary/null")
		} else {
			setUnknownNexusContent(h, payload.GetMetadata())
		}
		return nil
	}

	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		setUnknownNexusContent(h, payload.GetMetadata())
		return nil
	}

	switch mediaType {
	case "application/x-temporal-payload":
		err := payload.Unmarshal(content.Data)
		if err != nil {
			return serialization.NewDeserializationError(enumspb.ENCODING_TYPE_PROTO3, err)
		}
	case "application/json":
		if len(params) == 0 {
			payload.GetMetadata()["encoding"] = []byte("json/plain")
		} else if len(params) == 2 && params["format"] == "protobuf" && params["message-type"] != "" {
			payload.GetMetadata()["encoding"] = []byte("json/protobuf")
			payload.GetMetadata()["messageType"] = []byte(params["message-type"])
		} else {
			setUnknownNexusContent(h, payload.GetMetadata())
		}
	case "application/x-protobuf":
		if len(params) == 1 && params["message-type"] != "" {
			payload.GetMetadata()["encoding"] = []byte("binary/protobuf")
			payload.GetMetadata()["messageType"] = []byte(params["message-type"])
		} else {
			setUnknownNexusContent(h, payload.GetMetadata())
		}
	case "application/octet-stream":
		if len(params) == 0 {
			payload.GetMetadata()["encoding"] = []byte("binary/plain")
		} else {
			setUnknownNexusContent(h, payload.GetMetadata())
		}
	default:
		setUnknownNexusContent(h, payload.GetMetadata())
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

	// Use the "nil" Nexus Content representation for nil Payloads.
	if payload == nil {
		// Use same structure as the nil serializer from the Nexus Go SDK.
		return &nexus.Content{Header: nexus.Header{}}, nil
	}

	if len(payload.GetMetadata()) == 0 {
		return xTemporalPayload(payload)
	}

	content := nexus.Content{Header: nexus.Header{}, Data: payload.GetData()}
	encoding := string(payload.GetMetadata()["encoding"])
	messageType := string(payload.GetMetadata()["messageType"])

	switch encoding {
	case "unknown/nexus-content":
		for k, v := range payload.GetMetadata() {
			if k != "encoding" {
				content.Header[k] = string(v)
			}
		}
	case "json/protobuf":
		if len(payload.GetMetadata()) != 2 || messageType == "" {
			return xTemporalPayload(payload)
		}
		content.Header["type"] = fmt.Sprintf("application/json; format=protobuf; message-type=%q", messageType)
	case "binary/protobuf":
		if len(payload.GetMetadata()) != 2 || messageType == "" {
			return xTemporalPayload(payload)
		}
		content.Header["type"] = fmt.Sprintf("application/x-protobuf; message-type=%q", messageType)
	case "json/plain":
		content.Header["type"] = "application/json"
	case "binary/null":
		if len(payload.GetMetadata()) != 1 {
			return xTemporalPayload(payload)
		}
		// type is unset
	case "binary/plain":
		if len(payload.GetMetadata()) != 1 {
			return xTemporalPayload(payload)
		}
		content.Header["type"] = "application/octet-stream"
	default:
		return xTemporalPayload(payload)
	}

	return &content, nil
}

func xTemporalPayload(payload *commonpb.Payload) (*nexus.Content, error) {
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
