package interceptor

import (
	"context"
	"strings"

	"go.temporal.io/api/serviceerror"
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
		return i.omissionError(method, err)
	}
	return nil
}

func (i *DraftFieldsInterceptor) omissionError(method string, err error) error {
	i.logger.Error(draftFilteringFailure, tag.Operation(method), tag.Error(err))
	return status.Error(codes.Internal, draftFilteringFailure)
}

func (i *DraftFieldsInterceptor) omitOutgoingRoot(message proto.Message, selection *draft.Selection, method string) (proto.Message, error) {
	needed, err := draft.NeedsOmission(message, selection)
	if err != nil {
		return nil, i.omissionError(method, err)
	}
	if !needed {
		return nil, nil
	}
	filtered := proto.Clone(message)
	if err := i.omitRoot(filtered, selection, method); err != nil {
		return nil, err
	}
	return filtered, nil
}

func (i *DraftFieldsInterceptor) omitErrorDetails(original error, selection *draft.Selection, method string) error {
	st := serviceerror.ToStatus(original)
	var filteredDetails map[int][]byte
	for idx, detail := range st.Details() {
		message, ok := detail.(proto.Message)
		if !ok {
			continue
		}
		filtered, err := i.omitOutgoingRoot(message, selection, method)
		if err != nil {
			return err
		}
		if filtered == nil || proto.Equal(filtered, message) {
			continue
		}
		value, err := proto.Marshal(filtered)
		if err != nil {
			return i.omissionError(method, err)
		}
		if filteredDetails == nil {
			filteredDetails = make(map[int][]byte)
		}
		filteredDetails[idx] = value
	}
	if len(filteredDetails) == 0 {
		return original
	}
	updated := st.Proto()
	for idx, value := range filteredDetails {
		updated.Details[idx].Value = value
	}
	return status.FromProto(updated).Err()
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
		return resp, i.omitErrorDetails(err, invocation.selection, info.FullMethod)
	}
	message, ok := resp.(proto.Message)
	if !ok {
		return resp, nil
	}
	filtered, err := i.omitOutgoingRoot(message, invocation.selection, info.FullMethod)
	if err != nil {
		return nil, err
	}
	if filtered == nil {
		return resp, nil
	}
	return filtered, nil
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
