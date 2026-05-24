// This file is hand-written to work around a protoc-gen-go inconsistency:
// the enum constants in message.pb.go are prefix-stripped
// (STREAM_CLOSE_REASON_*), but the cross-file getter in
// request_response.pb.go references the non-stripped form
// (StreamCloseReason_STREAM_CLOSE_REASON_*).  We expose aliases under both
// names so the generated file compiles.  If a future protoc-gen-go release
// unifies the convention, this file can be deleted.
//
// NOTE: `make protoc` regenerates the gen directory and wipes this
// file.  If you regenerate, restore aliases.go before building.

package streampb

const (
	StreamCloseReason_STREAM_CLOSE_REASON_UNSPECIFIED         = STREAM_CLOSE_REASON_UNSPECIFIED
	StreamCloseReason_STREAM_CLOSE_REASON_EXPLICIT            = STREAM_CLOSE_REASON_EXPLICIT
	StreamCloseReason_STREAM_CLOSE_REASON_OWNER_WORKFLOW_DONE = STREAM_CLOSE_REASON_OWNER_WORKFLOW_DONE
	StreamCloseReason_STREAM_CLOSE_REASON_DELETED             = STREAM_CLOSE_REASON_DELETED
	StreamCloseReason_STREAM_CLOSE_REASON_NAMESPACE_DELETED   = STREAM_CLOSE_REASON_NAMESPACE_DELETED
)
