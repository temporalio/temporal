package testenv

import (
	"context"
	"reflect"
	"strings"
	"time"

	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/testing/stamp"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

func issueWorkflowRPC(
	ctx context.Context,
	c *Cluster,
	req proto.Message,
	actionID stamp.ActID,
) (proto.Message, error) {
	// tagging the request with an action ID to match it to the action (see clusterMonitor)
	md := metadata.Pairs(
		clusterIdKey, string(c.GetID()), // TODO: make this part of the FrontendClient so all requests are tagged
		actionIdKey, string(actionID),
	)
	ctx, cancel := context.WithTimeout(headers.SetVersions(metadata.NewOutgoingContext(ctx, md)), 5*time.Second) // TODO: tweak this
	defer cancel()

	res := reflect.ValueOf(c.physical.FrontendClient()).
		MethodByName(strings.TrimSuffix(string(req.ProtoReflect().Descriptor().Name()), "Request")).
		Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(req)})

	if res[0].IsNil() {
		return nil, res[1].Interface().(error)
	}
	return res[0].Interface().(proto.Message), nil
}
