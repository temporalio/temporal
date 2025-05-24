package testenv

import (
	"context"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
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

type sharedResource[T any] struct {
	startOnce sync.Once
	stopOnce  sync.Once
	value     T
	userCount atomic.Int32
	stopped   atomic.Bool
}

func (oc *sharedResource[T]) Start(fn func() T) T {
	oc.userCount.Add(1)
	oc.startOnce.Do(func() {
		oc.value = fn()
	})
	return oc.value
}

func (oc *sharedResource[T]) Get() T {
	if oc.stopped.Load() {
		panic("resource already stopped")
	}
	return oc.value
}

func (oc *sharedResource[T]) Stop(fn func(T)) {
	if oc.userCount.Add(-1) == 0 {
		oc.stopOnce.Do(func() {
			fn(oc.value)
			oc.stopped.Store(true)
		})
	}
}
