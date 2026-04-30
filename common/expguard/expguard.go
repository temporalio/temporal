// Package expguard checks that experimental proto fields don't cross stable
// service boundaries. The compile-time boundary (experimental symbols gated
// behind //go:build experimental) prevents stable code from *constructing*
// experimental values, but the protobuf wire format preserves them as unknown
// fields when an experimental client talks to a stable server. expguard
// detects that case at the boundary.
package expguard

import (
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/softassert"
	"google.golang.org/protobuf/proto"
)

// Check fires a softassert if msg carries any unknown proto fields.
//
// Unknown fields on an inbound request indicate an experimental client sent
// fields that this build doesn't recognise. In stable prod binaries this
// should never happen; in test binaries the test runner flags the assertion.
//
// site is a short static string describing the call site (e.g.
// "frontend.inbound", "replication.outbound") used for log grouping.
func Check(logger log.Logger, msg proto.Message, site string) {
	if msg == nil {
		return
	}
	if len(msg.ProtoReflect().GetUnknown()) == 0 {
		return
	}
	softassert.Fail(logger,
		"experimental unknown fields at stable boundary",
		tag.NewStringTag("site", site),
		tag.NewStringTag("message", string(msg.ProtoReflect().Descriptor().FullName())),
	)
}
