package internal

import (
	commonpb "go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/proto"
)

// IsEmptyPayload reports whether p carries no data or metadata. A workflow that
// returns no result reaches the scheduler's completion callback as an empty
// Payload; V1 treats that as "no result", and codecs reject a payload without
// encoding metadata.
func IsEmptyPayload(p *commonpb.Payload) bool {
	return proto.Size(p) == 0
}
