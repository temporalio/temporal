package interceptor

import (
	"context"
	"fmt"
	"strings"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/tasktoken"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

type (
	TaskTokenGetter interface {
		GetTaskToken() []byte
	}

	// NamespaceValidatorInterceptor contains NamespaceValidateIntercept and StateValidationIntercept
	NamespaceValidatorInterceptor struct {
		namespaceRegistry               namespace.Registry
		tokenSerializer                 *tasktoken.Serializer
		enableTokenNamespaceEnforcement dynamicconfig.BoolPropertyFn
		maxNamespaceLength              dynamicconfig.IntPropertyFn
		// Keyed by full gRPC method, like allowedMethodsDuringHandover.
		additionalAllowedMethodsDuringHandover map[string]struct{}
	}
)

var (
	errNamespaceNotSet            = serviceerror.NewInvalidArgument("Namespace not set on request.")
	errBothNamespaceIDAndNameSet  = serviceerror.NewInvalidArgument("Only one of namespace name or Id should be set on request.")
	errNamespaceTooLong           = serviceerror.NewInvalidArgument("Namespace length exceeds limit.")
	errTaskTokenNotSet            = serviceerror.NewInvalidArgument("Task token not set on request.")
	errTaskTokenNamespaceMismatch = serviceerror.NewInvalidArgument("Operation requested with a token from a different namespace.")
	errDeserializingToken         = serviceerror.NewInvalidArgument("Error deserializing task token.")

	allowedNamespaceStatesPerAPI = map[string][]enumspb.NamespaceState{
		api.WorkflowServicePrefix + "StartWorkflowExecution":           {enumspb.NAMESPACE_STATE_REGISTERED},
		api.WorkflowServicePrefix + "SignalWithStartWorkflowExecution": {enumspb.NAMESPACE_STATE_REGISTERED},
		api.OperatorServicePrefix + "DeleteNamespace":                  {enumspb.NAMESPACE_STATE_REGISTERED, enumspb.NAMESPACE_STATE_DEPRECATED, enumspb.NAMESPACE_STATE_DELETED},
		api.NexusServicePrefix + "DispatchNexusTask":                   {enumspb.NAMESPACE_STATE_REGISTERED},
	}
	// If API name is not in the map above, these are allowed states for all APIs of specific service
	// that have `namespace` or `task_token` field in the request object.
	allowedNamespaceStatesPerService = map[string][]enumspb.NamespaceState{
		api.AdminServicePrefix: {enumspb.NAMESPACE_STATE_REGISTERED, enumspb.NAMESPACE_STATE_DEPRECATED, enumspb.NAMESPACE_STATE_DELETED},
	}
	// If service name is not in the map above, these are allowed states for all APIs
	// that have `namespace` or `task_token` field in the request object.
	allowedNamespaceStatesDefault = []enumspb.NamespaceState{enumspb.NAMESPACE_STATE_REGISTERED, enumspb.NAMESPACE_STATE_DEPRECATED}

	// DO NOT allow workflow data read during namespace handover to prevent read-after-write inconsistency.
	//
	// Keyed by full gRPC method: the frontend serves three services plus an embedder's,
	// and GetSearchAttributes below is on two of them.
	allowedMethodsDuringHandover = map[string]struct{}{
		// System
		api.WorkflowServicePrefix + "GetSystemInfo":       {},
		api.WorkflowServicePrefix + "GetSearchAttributes": {},
		api.AdminServicePrefix + "GetSearchAttributes":    {},
		// Search attribute schema, not workflow data — the same information
		// GetSearchAttributes returns. DeleteNamespace stays out: it starts a workflow that
		// deletes the namespace's executions.
		api.OperatorServicePrefix + "ListSearchAttributes":   {},
		api.OperatorServicePrefix + "AddSearchAttributes":    {},
		api.OperatorServicePrefix + "RemoveSearchAttributes": {},
		api.WorkflowServicePrefix + "GetClusterInfo":         {},
		// Namespace APIs
		api.WorkflowServicePrefix + "DeprecateNamespace": {},
		api.WorkflowServicePrefix + "DescribeNamespace":  {},
		api.WorkflowServicePrefix + "UpdateNamespace":    {},
		api.WorkflowServicePrefix + "ListNamespaces":     {},
		api.WorkflowServicePrefix + "RegisterNamespace":  {},
		// Replication APIs
		api.AdminServicePrefix + "GetReplicationMessages":           {},
		api.AdminServicePrefix + "GetWorkflowExecutionRawHistory":   {},
		api.AdminServicePrefix + "GetWorkflowExecutionRawHistoryV2": {},
		// HistoryService is not served on the frontend, so this matches nothing today.
		api.HistoryServicePrefix + "ReplicateEventsV2": {},
		// Visibility APIs
		api.WorkflowServicePrefix + "ListTaskQueuePartitions":        {},
		api.WorkflowServicePrefix + "ListOpenWorkflowExecutions":     {},
		api.WorkflowServicePrefix + "ListClosedWorkflowExecutions":   {},
		api.WorkflowServicePrefix + "ListWorkflowExecutions":         {},
		api.WorkflowServicePrefix + "ListArchivedWorkflowExecutions": {},
		api.WorkflowServicePrefix + "ScanWorkflowExecutions":         {},
		api.WorkflowServicePrefix + "CountWorkflowExecutions":        {},
		api.WorkflowServicePrefix + "ListSchedules":                  {},
		api.WorkflowServicePrefix + "ListBatchOperations":            {},
		// Matching
		api.WorkflowServicePrefix + "ShutdownWorker": {},
	}
)

// newAdditionalAllowedMethods builds an embedder's handover allow-list from full gRPC
// methods. A bad entry is inert rather than fatal, and logged where the list is
// validated.
func newAdditionalAllowedMethods(methods []string) map[string]struct{} {
	out := make(map[string]struct{}, len(methods))
	for _, method := range methods {
		out[method] = struct{}{}
	}
	return out
}

// handoverAllowed reports whether fullMethod may proceed while its namespace is handing
// over.
func handoverAllowed(fullMethod string, additional map[string]struct{}) bool {
	if _, ok := allowedMethodsDuringHandover[fullMethod]; ok {
		return true
	}
	_, ok := additional[fullMethod]
	return ok
}

// validateFullMethods reports entries that do not name a real method, for the maps this
// package keys by full gRPC method. A service with no descriptor in this binary is
// skipped — Nexus has none, and service names come from constants.
func validateFullMethods(fullMethods ...string) error {
	var problems []string
	for _, fullMethod := range fullMethods {
		service, method, ok := api.ParseFullMethod(fullMethod)
		if !ok {
			problems = append(problems, fmt.Sprintf(
				"%q is not a full gRPC method, want \"/pkg.Service/Method\"", fullMethod))
			continue
		}
		found, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(service))
		if err != nil {
			continue
		}
		if descriptor, ok := found.(protoreflect.ServiceDescriptor); ok {
			if descriptor.Methods().ByName(protoreflect.Name(method)) == nil {
				problems = append(problems, fmt.Sprintf("%s has no method %q", service, method))
			}
		}
	}
	if len(problems) > 0 {
		return fmt.Errorf("invalid full gRPC methods: %s", strings.Join(problems, "; "))
	}
	return nil
}

var _ grpc.UnaryServerInterceptor = (*NamespaceValidatorInterceptor)(nil).StateValidationIntercept
var _ grpc.UnaryServerInterceptor = (*NamespaceValidatorInterceptor)(nil).NamespaceValidateIntercept

func NewNamespaceValidatorInterceptor(
	namespaceRegistry namespace.Registry,
	enableTokenNamespaceEnforcement dynamicconfig.BoolPropertyFn,
	maxNamespaceLength dynamicconfig.IntPropertyFn,
	additionalAllowedMethodsDuringHandover []string,
) *NamespaceValidatorInterceptor {
	additional := newAdditionalAllowedMethods(additionalAllowedMethodsDuringHandover)
	return &NamespaceValidatorInterceptor{
		namespaceRegistry:                      namespaceRegistry,
		tokenSerializer:                        tasktoken.NewSerializer(),
		enableTokenNamespaceEnforcement:        enableTokenNamespaceEnforcement,
		additionalAllowedMethodsDuringHandover: additional,
		maxNamespaceLength:                     maxNamespaceLength,
	}
}

func (ni *NamespaceValidatorInterceptor) NamespaceValidateIntercept(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	err := ni.setNamespaceIfNotPresent(req)
	if err != nil {
		return nil, err
	}
	reqWithNamespace, hasNamespace := req.(NamespaceNameGetter)
	if hasNamespace {
		if err := ni.ValidateName(reqWithNamespace.GetNamespace()); err != nil {
			return nil, err
		}
	}

	return handler(ctx, req)
}

// ValidateName validates a namespace name (currently only a max length check).
func (ni *NamespaceValidatorInterceptor) ValidateName(ns string) error {
	if len(ns) > ni.maxNamespaceLength() {
		return errNamespaceTooLong
	}
	return nil
}

func (ni *NamespaceValidatorInterceptor) setNamespaceIfNotPresent(
	req any,
) error {
	switch request := req.(type) {
	case NamespaceNameGetter:
		if request.GetNamespace() == "" {
			namespaceEntry, err := ni.extractNamespaceFromTaskToken(req)
			if err != nil {
				return err
			}
			ni.setNamespace(namespaceEntry, req)
		}
		return nil
	default:
		return nil
	}
}

func (ni *NamespaceValidatorInterceptor) setNamespace(
	namespaceEntry *namespace.Namespace,
	req any,
) {
	switch request := req.(type) {
	case *workflowservice.RespondQueryTaskCompletedRequest:
		if request.Namespace == "" {
			request.Namespace = namespaceEntry.Name().String()
		}
	case *workflowservice.RespondWorkflowTaskCompletedRequest:
		if request.Namespace == "" {
			request.Namespace = namespaceEntry.Name().String()
		}
	case *workflowservice.RespondWorkflowTaskFailedRequest:
		if request.Namespace == "" {
			request.Namespace = namespaceEntry.Name().String()
		}
	case *workflowservice.RecordActivityTaskHeartbeatRequest:
		if request.Namespace == "" {
			request.Namespace = namespaceEntry.Name().String()
		}
	case *workflowservice.RespondActivityTaskCanceledRequest:
		if request.Namespace == "" {
			request.Namespace = namespaceEntry.Name().String()
		}
	case *workflowservice.RespondActivityTaskCompletedRequest:
		if request.Namespace == "" {
			request.Namespace = namespaceEntry.Name().String()
		}
	case *workflowservice.RespondActivityTaskFailedRequest:
		if request.Namespace == "" {
			request.Namespace = namespaceEntry.Name().String()
		}
	case *workflowservice.RespondNexusTaskCompletedRequest:
		if request.Namespace == "" {
			request.Namespace = namespaceEntry.Name().String()
		}
	case *workflowservice.RespondNexusTaskFailedRequest:
		if request.Namespace == "" {
			request.Namespace = namespaceEntry.Name().String()
		}
	}
}

// StateValidationIntercept runs ValidateState - see docstring for that method.
func (ni *NamespaceValidatorInterceptor) StateValidationIntercept(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	namespaceEntry, err := ni.extractNamespace(req)
	if err != nil {
		return nil, err
	}

	if err := ni.ValidateState(namespaceEntry, info.FullMethod, GetRoutingKeyFromContext(ctx).ID); err != nil {
		return nil, err
	}

	return handler(ctx, req)
}

// ValidateState validates:
// 1. Namespace is specified in task token if there is a `task_token` field.
// 2. Namespace is specified in request if there is a `namespace` field and no `task_token` field.
// 3. Namespace exists.
// 4. Namespace from request match namespace from task token, if check is enabled with dynamic config.
// 5. Namespace is in correct state.
func (ni *NamespaceValidatorInterceptor) ValidateState(namespaceEntry *namespace.Namespace, fullMethod string, businessID string) error {
	if err := ni.checkNamespaceState(namespaceEntry, fullMethod); err != nil {
		return err
	}
	return ni.checkReplicationState(namespaceEntry, fullMethod, businessID)
}

func (ni *NamespaceValidatorInterceptor) extractNamespace(req any) (*namespace.Namespace, error) {
	// Token namespace has priority over request namespace. Check it first.
	tokenNamespaceEntry, tokenErr := ni.extractNamespaceFromTaskToken(req)
	if tokenErr != nil {
		return nil, tokenErr
	}

	requestNamespaceEntry, requestErr := ni.extractNamespaceFromRequest(req)
	// If namespace was extracted from token then it will be used.
	if requestErr != nil && tokenNamespaceEntry == nil {
		return nil, requestErr
	}

	err := ni.checkNamespaceMatch(requestNamespaceEntry, tokenNamespaceEntry)
	if err != nil {
		return nil, err
	}

	// Use namespace from task token (if specified) and ignore namespace from request.
	if tokenNamespaceEntry != nil {
		return tokenNamespaceEntry, nil
	}

	return requestNamespaceEntry, nil
}

func (ni *NamespaceValidatorInterceptor) extractNamespaceFromRequest(req any) (*namespace.Namespace, error) {
	reqWithNamespace, hasNamespace := req.(NamespaceNameGetter)
	if !hasNamespace {
		return nil, nil
	}
	namespaceName := namespace.Name(reqWithNamespace.GetNamespace())

	switch request := req.(type) {
	case *workflowservice.DescribeNamespaceRequest:
		// Special case for DescribeNamespace API which should read namespace directly from database.
		// Therefore, it must bypass namespace registry and validator.
		if request.GetId() == "" && namespaceName.IsEmpty() {
			return nil, errNamespaceNotSet
		}
		return nil, nil
	case *adminservice.GetNamespaceRequest:
		// special case for Admin.GetNamespace API which accept either Namespace ID or Namespace name as input
		if request.GetId() == "" && namespaceName.IsEmpty() {
			return nil, errNamespaceNotSet
		}
		return nil, nil
	case *workflowservice.RegisterNamespaceRequest:
		// Special case for RegisterNamespace API. `namespaceName` is name of namespace that about to be registered.
		// There is no namespace entry for it, therefore, it must bypass namespace registry and validator.
		if namespaceName.IsEmpty() {
			return nil, errNamespaceNotSet
		}
		return nil, nil
	case *operatorservice.DeleteNamespaceRequest:
		// special case for Operator.DeleteNamespace API which accept either Namespace ID or Namespace name as input
		namespaceID := namespace.ID(request.GetNamespaceId())
		if namespaceID.IsEmpty() && namespaceName.IsEmpty() {
			return nil, errNamespaceNotSet
		}
		if !namespaceID.IsEmpty() && !namespaceName.IsEmpty() {
			return nil, errBothNamespaceIDAndNameSet
		}
		if namespaceID != "" {
			return ni.namespaceRegistry.GetNamespaceByID(namespaceID)
		}
		return ni.namespaceRegistry.GetNamespace(namespaceName)
	case *adminservice.DescribeHistoryHostRequest:
		// Special case for DescribeHistoryHost API which should run regardless of namespace state.
		return nil, nil
	case *adminservice.AddSearchAttributesRequest,
		*adminservice.RemoveSearchAttributesRequest,
		*adminservice.GetSearchAttributesRequest,
		*operatorservice.AddSearchAttributesRequest,
		*operatorservice.RemoveSearchAttributesRequest,
		*operatorservice.ListSearchAttributesRequest:
		// Namespace is optional for search attributes operations.
		// It's required when using SQL DB for visibility, but not when using Elasticsearch.
		if !namespaceName.IsEmpty() {
			return ni.namespaceRegistry.GetNamespace(namespaceName)
		}
		return nil, nil
	default:
		// All other APIs.
		if namespaceName.IsEmpty() {
			return nil, errNamespaceNotSet
		}
		return ni.namespaceRegistry.GetNamespace(namespaceName)
	}
}

func (ni *NamespaceValidatorInterceptor) extractNamespaceFromTaskToken(req any) (*namespace.Namespace, error) {
	reqWithTaskToken, hasTaskToken := req.(TaskTokenGetter)
	if !hasTaskToken {
		return nil, nil
	}
	taskTokenBytes := reqWithTaskToken.GetTaskToken()
	if len(taskTokenBytes) == 0 {
		return nil, errTaskTokenNotSet
	}
	var namespaceID namespace.ID
	// Special case for deprecated RespondQueryTaskCompleted API.
	if _, ok := req.(*workflowservice.RespondQueryTaskCompletedRequest); ok {
		taskToken, err := ni.tokenSerializer.DeserializeQueryTaskToken(taskTokenBytes)
		if err != nil {
			return nil, errDeserializingToken
		}
		namespaceID = namespace.ID(taskToken.GetNamespaceId())
	} else {
		taskToken, err := ni.tokenSerializer.Deserialize(taskTokenBytes)
		if err != nil {
			return nil, errDeserializingToken
		}
		namespaceID = namespace.ID(taskToken.GetNamespaceId())
	}

	if namespaceID.IsEmpty() {
		return nil, errNamespaceNotSet
	}
	return ni.namespaceRegistry.GetNamespaceByID(namespaceID)
}

func (ni *NamespaceValidatorInterceptor) checkNamespaceMatch(requestNamespace *namespace.Namespace, tokenNamespace *namespace.Namespace) error {
	if tokenNamespace == nil || requestNamespace == nil || !ni.enableTokenNamespaceEnforcement() {
		return nil
	}

	if requestNamespace.ID() != tokenNamespace.ID() {
		return errTaskTokenNamespaceMismatch
	}
	return nil
}

func (ni *NamespaceValidatorInterceptor) checkNamespaceState(namespaceEntry *namespace.Namespace, fullMethod string) error {
	if namespaceEntry == nil {
		return nil
	}

	allowedStates, allowedStatesPerAPIDefined := allowedNamespaceStatesPerAPI[fullMethod]
	if !allowedStatesPerAPIDefined {
		serviceName := api.ServiceName(fullMethod)
		var allowedStatesPerServiceDefined bool
		allowedStates, allowedStatesPerServiceDefined = allowedNamespaceStatesPerService[serviceName]
		if !allowedStatesPerServiceDefined {
			allowedStates = allowedNamespaceStatesDefault
		}
	}
	for _, allowedState := range allowedStates {
		if allowedState == namespaceEntry.State() {
			return nil
		}
	}
	return serviceerror.NewNamespaceInvalidState(namespaceEntry.Name().String(), namespaceEntry.State(), allowedStates)
}

func (ni *NamespaceValidatorInterceptor) checkReplicationState(namespaceEntry *namespace.Namespace, fullMethod string, businessID string) error {
	if namespaceEntry == nil {
		return nil
	}
	if namespaceEntry.ReplicationState(businessID) != enumspb.REPLICATION_STATE_HANDOVER {
		return nil
	}

	if handoverAllowed(fullMethod, ni.additionalAllowedMethodsDuringHandover) {
		return nil
	}

	return common.ErrNamespaceHandover
}
