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
		namespaceRegistry      namespace.Registry
		timeSource             clock.TimeSource
		enabledForNS           dynamicconfig.BoolPropertyFnWithNamespaceFilter
		nsCacheRefreshInterval dynamicconfig.DurationPropertyFn
		metricsHandler         metrics.Handler
		logger                 log.Logger
		requestErrorHandler    ErrorHandler
	}
)

func NewNamespaceHandoverInterceptor(
	dc *dynamicconfig.Collection,
	namespaceRegistry namespace.Registry,
	metricsHandler metrics.Handler,
	logger log.Logger,
	timeSource clock.TimeSource,
	requestErrorHandler ErrorHandler,
) *NamespaceHandoverInterceptor {

	return &NamespaceHandoverInterceptor{
		enabledForNS:           dynamicconfig.EnableNamespaceHandoverWait.Get(dc),
		nsCacheRefreshInterval: dynamicconfig.NamespaceCacheRefreshInterval.Get(dc),
		namespaceRegistry:      namespaceRegistry,
		metricsHandler:         metricsHandler,
		logger:                 logger,
		timeSource:             timeSource,
		requestErrorHandler:    requestErrorHandler,
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
	namespaceName := MustGetNamespaceName(i.namespaceRegistry, req)

	if namespaceName != namespace.EmptyName && i.enabledForNS(namespaceName.String()) {
		var waitTime *time.Duration
		defer func() {
			if waitTime != nil {
				metrics.HandoverWaitLatency.With(i.metricsHandler).Record(*waitTime)
			}
		}()
		waitTime, err := i.waitNamespaceHandoverUpdate(ctx, namespaceName, methodName)
		if err != nil {
			metricsHandler, logTags := CreateUnaryMetricsHandlerLogTags(
				i.metricsHandler,
				req,
				info.FullMethod,
				methodName,
				namespaceName,
			)
			// count the request as this will not be counted
			metrics.ServiceRequests.With(metricsHandler).Record(1)

			i.requestErrorHandler.HandleError(
				req,
				info.FullMethod,
				metricsHandler,
				logTags,
				err,
				namespaceName,
			)
			return nil, err
		}
	}

	return handler(ctx, req)
}

func (i *NamespaceHandoverInterceptor) waitNamespaceHandoverUpdate(
	ctx context.Context,
	namespaceName namespace.Name,
	methodName string,
) (waitTime *time.Duration, retErr error) {
	if _, ok := allowedMethodsDuringHandover[methodName]; ok {
		return nil, nil
	}

	startTime := i.timeSource.Now()
	namespaceData, err := i.namespaceRegistry.GetNamespace(namespaceName)
	if err != nil {
		return nil, err
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

		maxWaitDuration := i.nsCacheRefreshInterval() // cache refresh time
		if deadline, ok := ctx.Deadline(); ok {
			maxWaitDuration = max(0, time.Until(deadline)-ctxTailRoom)
		}
		returnTimer := time.NewTimer(maxWaitDuration)
		var handoverErr error
		select {
		case <-returnTimer.C:
			handoverErr = common.ErrNamespaceHandover
		case <-waitReplicationStateUpdate:
			returnTimer.Stop()
		}
		i.namespaceRegistry.UnregisterStateChangeCallback(cbID)
		waitTime := time.Since(startTime)
		return &waitTime, handoverErr
	}
	return nil, nil
}
