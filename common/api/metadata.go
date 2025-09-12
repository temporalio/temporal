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

	MethodMetadata struct {
		// Describes the scope of a method (whole cluster or inividual namespace).
		Scope Scope
		// Describes what level of access is needed for a method (advisory).
		Access Access
		// Describes if long polling is supported by the method.
		Polling Polling
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
		"RegisterNamespace":                     {Scope: ScopeNamespace, Access: AccessAdmin, Polling: PollingNone},
		"DescribeNamespace":                     {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone},
		"ListNamespaces":                        {Scope: ScopeCluster, Access: AccessReadOnly, Polling: PollingNone},
		"UpdateNamespace":                       {Scope: ScopeNamespace, Access: AccessAdmin, Polling: PollingNone},
		"DeprecateNamespace":                    {Scope: ScopeNamespace, Access: AccessAdmin, Polling: PollingNone},
		"StartWorkflowExecution":                {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"GetWorkflowExecutionHistory":           {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingCapable},
		"GetWorkflowExecutionHistoryReverse":    {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone},
		"PollWorkflowTaskQueue":                 {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingAlways},
		"RespondWorkflowTaskCompleted":          {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"RespondWorkflowTaskFailed":             {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"PollActivityTaskQueue":                 {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingAlways},
		"RecordActivityTaskHeartbeat":           {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"RecordActivityTaskHeartbeatById":       {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"RespondActivityTaskCompleted":          {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"RespondActivityTaskCompletedById":      {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"RespondActivityTaskFailed":             {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"RespondActivityTaskFailedById":         {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"RespondActivityTaskCanceled":           {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"RespondActivityTaskCanceledById":       {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"PollNexusTaskQueue":                    {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingAlways},
		"RespondNexusTaskCompleted":             {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"RespondNexusTaskFailed":                {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"RequestCancelWorkflowExecution":        {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"SignalWorkflowExecution":               {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"SignalWithStartWorkflowExecution":      {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"ResetWorkflowExecution":                {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"TerminateWorkflowExecution":            {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"DeleteWorkflowExecution":               {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"ListOpenWorkflowExecutions":            {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone},
		"ListClosedWorkflowExecutions":          {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone},
		"ListWorkflowExecutions":                {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone},
		"ListArchivedWorkflowExecutions":        {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone},
		"ScanWorkflowExecutions":                {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone},
		"CountWorkflowExecutions":               {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone},
		"GetSearchAttributes":                   {Scope: ScopeCluster, Access: AccessReadOnly, Polling: PollingNone},
		"RespondQueryTaskCompleted":             {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"ResetStickyTaskQueue":                  {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"ShutdownWorker":                        {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"ExecuteMultiOperation":                 {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"QueryWorkflow":                         {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone},
		"DescribeWorkflowExecution":             {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone},
		"DescribeTaskQueue":                     {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone},
		"GetClusterInfo":                        {Scope: ScopeCluster, Access: AccessReadOnly, Polling: PollingNone},
		"GetSystemInfo":                         {Scope: ScopeCluster, Access: AccessReadOnly, Polling: PollingNone},
		"ListTaskQueuePartitions":               {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone},
		"CreateSchedule":                        {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"DescribeSchedule":                      {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone},
		"UpdateSchedule":                        {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"PatchSchedule":                         {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"ListScheduleMatchingTimes":             {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone},
		"DeleteSchedule":                        {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"ListSchedules":                         {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone},
		"UpdateWorkerBuildIdCompatibility":      {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"GetWorkerBuildIdCompatibility":         {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone},
		"UpdateWorkerVersioningRules":           {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"GetWorkerVersioningRules":              {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone},
		"GetWorkerTaskReachability":             {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone},
		"UpdateWorkflowExecution":               {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"PollWorkflowExecutionUpdate":           {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingAlways},
		"StartBatchOperation":                   {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"StopBatchOperation":                    {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"DescribeBatchOperation":                {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone},
		"ListBatchOperations":                   {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone},
		"UpdateActivityOptions":                 {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"PauseActivity":                         {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"UnpauseActivity":                       {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"ResetActivity":                         {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"UpdateWorkflowExecutionOptions":        {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"DescribeDeployment":                    {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone}, // [cleanup-wv-pre-release]
		"ListDeployments":                       {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone}, // [cleanup-wv-pre-release]
		"GetDeploymentReachability":             {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone}, // [cleanup-wv-pre-release]
		"GetCurrentDeployment":                  {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone}, // [cleanup-wv-pre-release]
		"SetCurrentDeployment":                  {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},    // [cleanup-wv-pre-release]
		"DescribeWorkerDeploymentVersion":       {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone},
		"DescribeWorkerDeployment":              {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone},
		"SetWorkerDeploymentCurrentVersion":     {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"SetWorkerDeploymentRampingVersion":     {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"SetWorkerDeploymentManager":            {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"DeleteWorkerDeployment":                {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"DeleteWorkerDeploymentVersion":         {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"UpdateWorkerDeploymentVersionMetadata": {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"ListWorkerDeployments":                 {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone},
		"CreateWorkflowRule":                    {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"DescribeWorkflowRule":                  {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone},
		"DeleteWorkflowRule":                    {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"ListWorkflowRules":                     {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone},
		"TriggerWorkflowRule":                   {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"RecordWorkerHeartbeat":                 {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"ListWorkers":                           {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone},
		"DescribeWorker":                        {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone},
		"UpdateTaskQueueConfig":                 {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
		"FetchWorkerConfig":                     {Scope: ScopeNamespace, Access: AccessReadOnly, Polling: PollingNone},
		"UpdateWorkerConfig":                    {Scope: ScopeNamespace, Access: AccessWrite, Polling: PollingNone},
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
