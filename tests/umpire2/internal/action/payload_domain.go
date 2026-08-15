package action

import (
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	umpire "go.temporal.io/server/common/testing/umpire"
	"google.golang.org/protobuf/proto"
)

// payloadDomain models payload metadata shape and a bounded encoded size.
type payloadDomain struct {
	maxBytes int
}

func newPayloadDomain(maxBytes int) *payloadDomain {
	return &payloadDomain{maxBytes: maxBytes}
}

func (d *payloadDomain) Variants() []umpire.Variant {
	return []umpire.Variant{
		{
			Label: "missing-encoding", Class: umpire.Malformed, Expect: &umpire.Reject{},
			Mutate: func(valid any) any {
				payload := clonePayload(valid)
				delete(payload.Metadata, "encoding")
				return payload
			},
		},
		{
			Label: "unknown-encoding", Class: umpire.Malformed, Expect: &umpire.Reject{},
			Mutate: func(valid any) any {
				payload := clonePayload(valid)
				if payload.Metadata == nil {
					payload.Metadata = map[string][]byte{}
				}
				payload.Metadata["encoding"] = []byte("umpire/unknown")
				return payload
			},
		},
		{
			Label: "too-large", Class: umpire.OutOfRange, Expect: &umpire.Reject{},
			Mutate: func(valid any) any {
				payload := clonePayload(valid)
				size := d.maxBytes + 1
				if size < 1 {
					size = 1
				}
				payload.Data = make([]byte, size)
				return payload
			},
		},
	}
}

func clonePayload(value any) *commonpb.Payload {
	payload, _ := value.(*commonpb.Payload)
	if payload == nil {
		return &commonpb.Payload{}
	}
	return proto.Clone(payload).(*commonpb.Payload)
}

func (d *payloadDomain) Normalize(value any) (string, error) {
	payload, ok := value.(*commonpb.Payload)
	if !ok || payload == nil {
		return "", fmt.Errorf("%w: payload has type %T", umpire.ErrDomainValue, value)
	}
	if d.maxBytes > 0 && len(payload.GetData()) > d.maxBytes {
		return "", fmt.Errorf("%w: payload has %d bytes, maximum is %d", umpire.ErrDomainValue, len(payload.GetData()), d.maxBytes)
	}
	return umpire.CanonicalProtoDigest("payload", payload)
}
