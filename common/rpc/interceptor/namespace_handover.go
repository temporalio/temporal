package interceptor

import (
	"context"
	"strings"
	"time"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/grpc"
)

const (
	ctxTailRoom = time.Millisecond * 100
)

var _ grpc.UnaryServerInterceptor = (*NamespaceHandoverInterceptor)(nil).Intercept

type (
	// NamespaceHandoverInterceptor handles the namespace in handover replication state
	NamespaceHandoverInterceptor struct {
		namespaceRegistry namespace.Registry
		timeSource        clock.TimeSource
		enabledForNS      dynamicconfig.BoolPropertyFnWithNamespaceFilter
		metricsHandler    metrics.Handler
		logger            log.Logger
	}
)

func NewNamespaceHandoverInterceptor(
	enabledForNS dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	namespaceRegistry namespace.Registry,
	metricsHandler metrics.Handler,
	logger log.Logger,
	timeSource clock.TimeSource,
) *NamespaceHandoverInterceptor {

	return &NamespaceHandoverInterceptor{
		enabledForNS:      enabledForNS,
		namespaceRegistry: namespaceRegistry,
		metricsHandler:    metricsHandler,
		logger:            logger,
		timeSource:        timeSource,
	}
}

func (i *NamespaceHandoverInterceptor) Intercept(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (_ any, retError error) {
	defer log.CapturePanic(i.logger, &retError)

	if !strings.HasPrefix(info.FullMethod, api.WorkflowServicePrefix) {
		return handler(ctx, req)
	}

	// review which method is allowed
	methodName := api.MethodName(info.FullMethod)
	namespaceName, err := GetNamespaceName(i.namespaceRegistry, req)
	if err != nil {
		return nil, err
	}

	if i.enabledForNS(namespaceName.String()) {
		startTime := i.timeSource.Now()
		defer func() {
			metrics.HandoverWaitLatency.With(i.metricsHandler).Record(time.Since(startTime))
		}()
		_, err = i.waitNamespaceHandoverUpdate(ctx, namespaceName, methodName)
		if err != nil {
			return nil, err
		}
	}

	return handler(ctx, req)
}

func (i *NamespaceHandoverInterceptor) waitNamespaceHandoverUpdate(
	ctx context.Context,
	namespaceName namespace.Name,
	methodName string,
) (waitTime time.Duration, retErr error) {
	if _, ok := allowedMethodsDuringHandover[methodName]; ok {
		return
	}

	startTime := time.Now()
	namespaceData, err := i.namespaceRegistry.GetNamespace(namespaceName)
	if err != nil {
		return 0, err
	}
	if namespaceData.ReplicationState() == enumspb.REPLICATION_STATE_HANDOVER {
		cbID := uuid.New()
		waitReplicationStateUpdate := make(chan struct{})
		i.namespaceRegistry.RegisterStateChangeCallback(cbID, func(ns *namespace.Namespace, deletedFromDb bool) {
			if ns.ID().String() != namespaceData.ID().String() {
				return
			}
			if ns.State() != enumspb.NAMESPACE_STATE_REGISTERED ||
				deletedFromDb ||
				ns.ReplicationState() != enumspb.REPLICATION_STATE_HANDOVER ||
				!ns.IsGlobalNamespace() {
				// Stop wait on state change if:
				// 1. namespace is deleting/deleted
				// 2. namespace is not in handover
				// 3. namespace is not global
				select {
				case <-waitReplicationStateUpdate:
				default:
					close(waitReplicationStateUpdate)
				}
			}
		})

		childCtx := context.Background()
		if deadline, ok := ctx.Deadline(); ok {
			var childCancelFn context.CancelFunc
			childCtx, childCancelFn = context.WithDeadline(ctx, deadline.Add(-ctxTailRoom))
			defer func() {
				childCancelFn()
			}()
		}
		select {
		case <-childCtx.Done():
			err = common.ErrNamespaceHandover
		case <-waitReplicationStateUpdate:
		}
		i.namespaceRegistry.UnregisterStateChangeCallback(cbID)
		if err != nil {
			// error?
			return time.Since(startTime), err
		}
	}
	return time.Since(startTime), nil
}
