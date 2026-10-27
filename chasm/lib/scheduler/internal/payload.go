package internal

import (
	commonpb "go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/proto"
)

// IsEmptyPayload reports whether p carries no data or metadata. Callback senders
// now deliver a no-result workflow as binary/null, but older senders delivered an
// empty Payload, which may still be in flight or persisted. Codecs reject a
// payload without encoding metadata.
func IsEmptyPayload(p *commonpb.Payload) bool {
	return proto.Size(p) == 0
}
