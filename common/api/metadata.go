package api

import "strings"

type (
	// Describes the scope of a method (whole cluster or inividual namespace).
	Scope int32

	// Describes what level of access is needed for a method. Note that this field is
	// completely advisory. Any authorizer implementation may implement whatever logic it
	// chooses, including ignoring this field. It is used by the "default" authorizer to check
	// against roles in claims.
	Access int32

	// Describes if the method supports long-polled requests.
	Polling int32

	// Redirection describes whether a method is forwarded to the namespace's active cluster when
	// cross-cluster (XDC) redirection is restricted to selected APIs.
	Redirection int32

	MethodMetadata struct {
		// Describes the scope of a method (whole cluster or inividual namespace).
		Scope Scope
		// Describes what level of access is needed for a method (advisory).
		Access Access
		// Describes if long polling is supported by the method.
		Polling Polling
		// Describes whether the method is forwarded to the namespace's active cluster under the
		// selected-apis-forwarding redirection policy. Only meaningful for WorkflowService methods;
		// see common/rpc/interceptor/dc_redirection_policy.go.
		Redirection Redirection
	}
)

const (
	// Represents a missing Scope value.
	ScopeUnknown Scope = iota
	// Method affects a single namespace. The request message must contain a string field named "Namespace".
	ScopeNamespace
	// Method affects the whole cluster. The request message must _not_ contain any field named "Namespace".
	ScopeCluster
)

const (
	// Represents a missing Access value.
	AccessUnknown Access = iota
	// Method is read-only and should be accessible to readers.
	AccessReadOnly
	// Method is a normal write method.
	AccessWrite
	// Method is an administrative operation.
	AccessAdmin
)

const (
	// Represents a missing Polling value.
	PollingUnknown Polling = iota
	// Method isn't capable of long-polling.
	PollingNone
	// Method can optionally return long-polled responses.
	PollingCapable
	// Method responses are always long-polled.
	PollingAlways
)

const (
	// Represents a missing Redirection value. Every WorkflowService method must declare a value
	// other than this; TestWorkflowServiceMetadata enforces it so a newly added API cannot silently
	// default to a redirection behavior without a deliberate choice.
	RedirectionUnknown Redirection = iota
	// Method is served locally on the receiving cluster; it is not forwarded to the active cluster
	// under the selected-apis-forwarding policy.
	RedirectionLocal
	// Method must be forwarded to the namespace's active cluster (it mutates state owned by, or must
	// observe, the active cluster) even when forwarding is restricted to selected APIs.
	RedirectionForwarding
)

const (
	WorkflowServicePrefix = "/temporal.api.workflowservice.v1.WorkflowService/"
	OperatorServicePrefix = "/temporal.api.operatorservice.v1.OperatorService/"
	HistoryServicePrefix  = "/temporal.server.api.historyservice.v1.HistoryService/"
	AdminServicePrefix    = "/temporal.server.api.adminservice.v1.AdminService/"
	MatchingServicePrefix = "/temporal.server.api.matchingservice.v1.MatchingService/"
	// Technically not a gRPC service, but still using this format for metadata.
	NexusServicePrefix = "/temporal.api.nexusservice.v1.NexusService/"
)

var (
	workflowServiceMetadata = map[string]MethodMetadata{
		"RegisterNamespace":                            {Scope: ScopeNamespace, Access: AccessAdmin, Polling: PollingNone, Redirection: RedirectionLocal},
		"DescribeNamespace":                            {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone, Redirection: RedirectionLocal},
		"ListNamespaces":                               {Scope: ScopeCluster, Access: AccessReadOnly, Polling: PollingNone, Redirection: RedirectionLocal},
		"UpdateNamespace":                              {Scope: ScopeNamespace, Access: AccessAdmin, Polling: PollingNone, Redirection: RedirectionLocal},
		"DeprecateNamespace":                           {Scope: ScopeNamespace, Access: AccessAdmin, Polling: PollingNone, Redirection: RedirectionLocal},
		"StartWorkflowExecution":                       {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionForwarding},
		"GetWorkflowExecutionHistory":                  {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingCapable, Redirection: RedirectionLocal},
		"GetWorkflowExecutionHistoryReverse":           {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone, Redirection: RedirectionLocal},
		"PollWorkflowTaskQueue":                        {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingAlways, Redirection: RedirectionLocal},
		"RespondWorkflowTaskCompleted":                 {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"RespondWorkflowTaskFailed":                    {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"PollActivityTaskQueue":                        {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingAlways, Redirection: RedirectionLocal},
		"RecordActivityTaskHeartbeat":                  {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"RecordActivityTaskHeartbeatById":              {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"RespondActivityTaskCompleted":                 {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"RespondActivityTaskCompletedById":             {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"RespondActivityTaskFailed":                    {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"RespondActivityTaskFailedById":                {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"RespondActivityTaskCanceled":                  {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"RespondActivityTaskCanceledById":              {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"CountActivityExecutions":                      {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone, Redirection: RedirectionLocal},
		"CountNexusOperationExecutions":                {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone, Redirection: RedirectionLocal},
		"DeleteActivityExecution":                      {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionForwarding},
		"DeleteNexusOperationExecution":                {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionForwarding},
		"DescribeActivityExecution":                    {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingCapable, Redirection: RedirectionLocal},
		"DescribeNexusOperationExecution":              {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingCapable, Redirection: RedirectionLocal},
		"PollActivityExecution":                        {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingAlways, Redirection: RedirectionLocal},
		"PollNexusOperationExecution":                  {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingAlways, Redirection: RedirectionLocal},
		"ListActivityExecutions":                       {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone, Redirection: RedirectionLocal},
		"ListNexusOperationExecutions":                 {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone, Redirection: RedirectionLocal},
		"RequestCancelActivityExecution":               {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionForwarding},
		"RequestCancelNexusOperationExecution":         {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionForwarding},
		"StartActivityExecution":                       {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionForwarding},
		"StartNexusOperationExecution":                 {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionForwarding},
		"TerminateActivityExecution":                   {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionForwarding},
		"TerminateNexusOperationExecution":             {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionForwarding},
		"PollNexusTaskQueue":                           {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingAlways, Redirection: RedirectionLocal},
		"RespondNexusTaskCompleted":                    {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"RespondNexusTaskFailed":                       {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"RequestCancelWorkflowExecution":               {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionForwarding},
		"SignalWorkflowExecution":                      {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionForwarding},
		"SignalWithStartWorkflowExecution":             {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionForwarding},
		"ResetWorkflowExecution":                       {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"TerminateWorkflowExecution":                   {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionForwarding},
		"DeleteWorkflowExecution":                      {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionForwarding},
		"ListOpenWorkflowExecutions":                   {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone, Redirection: RedirectionLocal},
		"ListClosedWorkflowExecutions":                 {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone, Redirection: RedirectionLocal},
		"ListWorkflowExecutions":                       {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone, Redirection: RedirectionLocal},
		"ListArchivedWorkflowExecutions":               {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone, Redirection: RedirectionLocal},
		"ScanWorkflowExecutions":                       {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone, Redirection: RedirectionLocal},
		"CountWorkflowExecutions":                      {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone, Redirection: RedirectionLocal},
		"GetSearchAttributes":                          {Scope: ScopeCluster, Access: AccessReadOnly, Polling: PollingNone, Redirection: RedirectionLocal},
		"RespondQueryTaskCompleted":                    {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"ResetStickyTaskQueue":                         {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"ShutdownWorker":                               {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"ExecuteMultiOperation":                        {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"QueryWorkflow":                                {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone, Redirection: RedirectionForwarding},
		"DescribeWorkflowExecution":                    {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone, Redirection: RedirectionLocal},
		"DescribeTaskQueue":                            {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone, Redirection: RedirectionLocal},
		"GetClusterInfo":                               {Scope: ScopeCluster, Access: AccessReadOnly, Polling: PollingNone, Redirection: RedirectionLocal},
		"GetSystemInfo":                                {Scope: ScopeCluster, Access: AccessReadOnly, Polling: PollingNone, Redirection: RedirectionLocal},
		"ListTaskQueuePartitions":                      {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone, Redirection: RedirectionLocal},
		"CreateSchedule":                               {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal}, // TODO: should be RedirectionForwarding (schedule mutation runs on the active cluster); flip in a follow-up
		"DescribeSchedule":                             {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone, Redirection: RedirectionLocal},
		"UpdateSchedule":                               {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal}, // TODO: should be RedirectionForwarding (schedule mutation runs on the active cluster); flip in a follow-up
		"PatchSchedule":                                {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal}, // TODO: should be RedirectionForwarding (schedule mutation runs on the active cluster); flip in a follow-up
		"ListScheduleMatchingTimes":                    {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone, Redirection: RedirectionLocal},
		"DeleteSchedule":                               {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal}, // TODO: should be RedirectionForwarding (schedule mutation runs on the active cluster); flip in a follow-up
		"ListSchedules":                                {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone, Redirection: RedirectionLocal},
		"CountSchedules":                               {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone, Redirection: RedirectionLocal},
		"UpdateWorkerBuildIdCompatibility":             {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"GetWorkerBuildIdCompatibility":                {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone, Redirection: RedirectionLocal},
		"UpdateWorkerVersioningRules":                  {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"GetWorkerVersioningRules":                     {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone, Redirection: RedirectionLocal},
		"GetWorkerTaskReachability":                    {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone, Redirection: RedirectionLocal},
		"UpdateWorkflowExecution":                      {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"PollWorkflowExecutionUpdate":                  {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingAlways, Redirection: RedirectionLocal},
		"StartBatchOperation":                          {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"StopBatchOperation":                           {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"DescribeBatchOperation":                       {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone, Redirection: RedirectionLocal},
		"ListBatchOperations":                          {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone, Redirection: RedirectionLocal},
		"UpdateActivityOptions":                        {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"PauseActivity":                                {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"UnpauseActivity":                              {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"ResetActivity":                                {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"UpdateActivityExecutionOptions":               {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"PauseActivityExecution":                       {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"UnpauseActivityExecution":                     {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"ResetActivityExecution":                       {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"UpdateWorkflowExecutionOptions":               {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"DescribeDeployment":                           {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone, Redirection: RedirectionLocal}, // [cleanup-wv-pre-release]
		"ListDeployments":                              {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone, Redirection: RedirectionLocal}, // [cleanup-wv-pre-release]
		"GetDeploymentReachability":                    {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone, Redirection: RedirectionLocal}, // [cleanup-wv-pre-release]
		"GetCurrentDeployment":                         {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone, Redirection: RedirectionLocal}, // [cleanup-wv-pre-release]
		"SetCurrentDeployment":                         {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},    // [cleanup-wv-pre-release]
		"DescribeWorkerDeploymentVersion":              {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone, Redirection: RedirectionLocal},
		"DescribeWorkerDeployment":                     {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone, Redirection: RedirectionLocal},
		"SetWorkerDeploymentCurrentVersion":            {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"SetWorkerDeploymentRampingVersion":            {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"SetWorkerDeploymentManager":                   {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"CreateWorkerDeployment":                       {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"DeleteWorkerDeployment":                       {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"CreateWorkerDeploymentVersion":                {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"UpdateWorkerDeploymentVersionComputeConfig":   {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"ValidateWorkerDeploymentVersionComputeConfig": {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone, Redirection: RedirectionLocal},
		"DeleteWorkerDeploymentVersion":                {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"UpdateWorkerDeploymentVersionMetadata":        {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"ListWorkerDeployments":                        {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone, Redirection: RedirectionLocal},
		"CreateWorkflowRule":                           {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"DescribeWorkflowRule":                         {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone, Redirection: RedirectionLocal},
		"DeleteWorkflowRule":                           {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"ListWorkflowRules":                            {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone, Redirection: RedirectionLocal},
		"TriggerWorkflowRule":                          {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"RecordWorkerHeartbeat":                        {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"ListWorkers":                                  {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone, Redirection: RedirectionLocal},
		"CountWorkers":                                 {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone, Redirection: RedirectionLocal},
		"DescribeWorker":                               {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone, Redirection: RedirectionLocal},
		"UpdateTaskQueueConfig":                        {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"FetchWorkerConfig":                            {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone, Redirection: RedirectionLocal},
		"UpdateWorkerConfig":                           {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"PauseWorkflowExecution":                       {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
		"UnpauseWorkflowExecution":                     {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone, Redirection: RedirectionLocal},
	}
	operatorServiceMetadata = map[string]MethodMetadata{
		"AddSearchAttributes":      {Scope: ScopeNamespace, Access: AccessAdmin, Polling: PollingNone},
		"RemoveSearchAttributes":   {Scope: ScopeNamespace, Access: AccessAdmin, Polling: PollingNone},
		"ListSearchAttributes":     {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone},
		"DeleteNamespace":          {Scope: ScopeNamespace, Access: AccessAdmin, Polling: PollingNone},
		"AddOrUpdateRemoteCluster": {Scope: ScopeCluster, Access: AccessAdmin, Polling: PollingNone},
		"RemoveRemoteCluster":      {Scope: ScopeCluster, Access: AccessAdmin, Polling: PollingNone},
		"ListClusters":             {Scope: ScopeCluster, Access: AccessAdmin, Polling: PollingNone},
		"CreateNexusEndpoint":      {Scope: ScopeCluster, Access: AccessAdmin, Polling: PollingNone},
		"UpdateNexusEndpoint":      {Scope: ScopeCluster, Access: AccessAdmin, Polling: PollingNone},
		"DeleteNexusEndpoint":      {Scope: ScopeCluster, Access: AccessAdmin, Polling: PollingNone},
		"GetNexusEndpoint":         {Scope: ScopeCluster, Access: AccessAdmin, Polling: PollingNone},
		"ListNexusEndpoints":       {Scope: ScopeCluster, Access: AccessAdmin, Polling: PollingNone},
	}
	nexusServiceMetadata = map[string]MethodMetadata{
		"DispatchNexusTask":               {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"DispatchByNamespaceAndTaskQueue": {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"DispatchByEndpoint":              {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"CompleteNexusOperation":          {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"CompleteNexusOperationChasm":     {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
	}
)

// GetMethodMetadata gets metadata for a given API method in one of the services exported by
// frontend (WorkflowService, OperatorService, AdminService).
func GetMethodMetadata(fullApiName string) MethodMetadata {
	switch {
	case strings.HasPrefix(fullApiName, WorkflowServicePrefix):
		return workflowServiceMetadata[MethodName(fullApiName)]
	case strings.HasPrefix(fullApiName, OperatorServicePrefix):
		return operatorServiceMetadata[MethodName(fullApiName)]
	case strings.HasPrefix(fullApiName, NexusServicePrefix):
		return nexusServiceMetadata[MethodName(fullApiName)]
	case strings.HasPrefix(fullApiName, AdminServicePrefix):
		return MethodMetadata{Scope: ScopeCluster, Access: AccessAdmin}
	default:
		return MethodMetadata{Scope: ScopeUnknown, Access: AccessUnknown}
	}
}

// WorkflowServiceForwardedAPIs returns the set of WorkflowService method names (bare names, not
// fully qualified) that must be forwarded to the namespace's active cluster under the
// selected-apis-forwarding redirection policy, i.e. those with Redirection == RedirectionForwarding.
// It is the single source of truth for that whitelist; the DC redirection interceptor derives its
// whitelist from this rather than maintaining a parallel list.
func WorkflowServiceForwardedAPIs() map[string]struct{} {
	forwarded := make(map[string]struct{})
	for method, md := range workflowServiceMetadata {
		if md.Redirection == RedirectionForwarding {
			forwarded[method] = struct{}{}
		}
	}
	return forwarded
}

// MethodName returns just the method name from a fully qualified name.
func MethodName(fullApiName string) string {
	index := strings.LastIndex(fullApiName, "/")
	if index > -1 {
		return fullApiName[index+1:]
	}
	return fullApiName
}

func ServiceName(fullApiName string) string {
	index := strings.LastIndex(fullApiName, "/")
	if index > -1 {
		return fullApiName[:index+1]
	}
	return ""
}
