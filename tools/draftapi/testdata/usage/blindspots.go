package usage

import (
	pb "go.temporal.io/api/testdraft/v1"
	"go.temporal.io/server/bridge"
)

type draftGetter interface {
	GetDraft() string
}

func interfaceMethod(req *pb.Stable) string {
	var getter draftGetter = req
	// The analyzer intentionally does not report calls through an interface.
	return getter.GetDraft()
}

func reflectField(req *pb.Stable) string {
	msg := req.ProtoReflect()
	field := msg.Descriptor().Fields().ByName("draft")
	// The analyzer intentionally does not report field access through protobuf reflection.
	return msg.Get(field).String()
}

func unkeyedLiteral() {
	// The analyzer intentionally does not report aliases from other server packages
	// or fields initialized positionally.
	_ = bridge.DraftChoice{"value"}
}

func genericGetter[T draftGetter](req T) string {
	// The analyzer intentionally does not report calls through a generic constraint.
	return req.GetDraft()
}

func genericCall(req *pb.Stable) string {
	return genericGetter(req)
}
