package testenv

import (
	"context"
	"reflect"
	"strings"
	"time"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/testing/stamp"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

func issueWorkflowRPC(
	ctx context.Context,
	client workflowservice.WorkflowServiceClient,
	req proto.Message,
	actionID stamp.ActID,
) (proto.Message, error) {
	// tagging the request with an action ID to match it to the action (see clusterMonitor)
	md := metadata.Pairs(actionIdKey, string(actionID))
	ctx, cancel := context.WithTimeout(headers.SetVersions(metadata.NewOutgoingContext(ctx, md)), 5*time.Second) // TODO: tweak this
	defer cancel()

	res := reflect.ValueOf(client).
		MethodByName(strings.TrimSuffix(string(req.ProtoReflect().Descriptor().Name()), "Request")).
		Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(req)})

	if res[0].IsNil() {
		return nil, res[1].Interface().(error)
	}
	return res[0].Interface().(proto.Message), nil
}
