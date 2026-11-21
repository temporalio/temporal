package interceptor

import (
	"context"
	"strings"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/draft"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const draftFilteringFailure = "draft field omission failed"

type DraftFieldsInterceptor struct {
	enabled  dynamicconfig.TypedPropertyFnWithNamespaceFilter[[]string]
	registry namespace.Registry
	logger   log.Logger
	omit     func(any, *draft.Selection) (bool, error)
}

type draftInvocationKey struct{}
type draftInvocation struct{ selection *draft.Selection }

func NewDraftFieldsInterceptor(
	enabled dynamicconfig.TypedPropertyFnWithNamespaceFilter[[]string],
	registry namespace.Registry,
	logger log.Logger,
	omit func(any, *draft.Selection) (bool, error),
) *DraftFieldsInterceptor {
	return &DraftFieldsInterceptor{enabled: enabled, registry: registry, logger: logger, omit: omit}
}

func draftMethod(method string) bool {
	return strings.HasPrefix(method, api.WorkflowServicePrefix) || strings.HasPrefix(method, api.OperatorServicePrefix)
}

func (i *DraftFieldsInterceptor) selection(req any) *draft.Selection {
	var name, id string
	if named, ok := req.(NamespaceNameGetter); ok {
		name = named.GetNamespace()
	}
	if identified, ok := req.(NamespaceIDGetter); ok {
		id = identified.GetNamespaceId()
	}
	if described, ok := req.(*workflowservice.DescribeNamespaceRequest); ok {
		id = described.GetId()
	}
	return draft.NewSelection(func() []string {
		if name != "" {
			return i.enabled(name)
		}
		if id != "" {
			name, err := i.registry.GetNamespaceName(namespace.ID(id))
			if err != nil {
				return nil
			}
			return i.enabled(name.String())
		}
		return i.enabled("")
	})
}

func (i *DraftFieldsInterceptor) omitRoot(msg any, selection *draft.Selection, method string) error {
	if _, ok := msg.(proto.Message); !ok {
		return nil
	}
	if _, err := i.omit(msg, selection); err != nil {
		i.logger.Error(draftFilteringFailure, tag.Operation(method))
		return status.Error(codes.Internal, draftFilteringFailure)
	}
	return nil
}

// OutgoingIntercept wraps the full chain, including successful early returns.
func (i *DraftFieldsInterceptor) OutgoingIntercept(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if !draftMethod(info.FullMethod) {
		return handler(ctx, req)
	}
	invocation := &draftInvocation{selection: i.selection(req)}
	ctx = context.WithValue(ctx, draftInvocationKey{}, invocation)
	resp, err := handler(ctx, req)
	if err != nil {
		return resp, err
	}
	if err := i.omitRoot(resp, invocation.selection, info.FullMethod); err != nil {
		return nil, err
	}
	return resp, nil
}

// IncomingIntercept runs immediately after existing namespace normalization.
func (i *DraftFieldsInterceptor) IncomingIntercept(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if !draftMethod(info.FullMethod) {
		return handler(ctx, req)
	}
	selection := i.selection(req)
	if invocation, ok := ctx.Value(draftInvocationKey{}).(*draftInvocation); ok {
		invocation.selection = selection
	}
	if err := i.omitRoot(req, selection, info.FullMethod); err != nil {
		return nil, err
	}
	return handler(ctx, req)
}
