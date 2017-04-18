package frontend

import (
	"log"
	"sync"

	"github.com/uber/cadence/.gen/go/cadence"
	h "github.com/uber/cadence/.gen/go/history"
	m "github.com/uber/cadence/.gen/go/matching"
	gen "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client/history"
	"github.com/uber/cadence/client/matching"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service"

	"github.com/uber-common/bark"
	"github.com/uber/tchannel-go/thrift"
)

var _ cadence.TChanWorkflowService = (*WorkflowHandler)(nil)

// WorkflowHandler - Thrift handler inteface for workflow service
type WorkflowHandler struct {
	domainCache     cache.DomainCache
	metadataMgr     persistence.MetadataManager
	historyMgr      persistence.HistoryManager
	visibitiltyMgr  persistence.VisibilityManager
	history         history.Client
	matching        matching.Client
	tokenSerializer common.TaskTokenSerializer
	hSerializer     common.HistorySerializer
	startWG         sync.WaitGroup
	service.Service
}

var (
	errDomainNotSet    = &gen.BadRequestError{Message: "Domain not set on request."}
	errTaskTokenNotSet = &gen.BadRequestError{Message: "Task token not set on request."}
)

// NewWorkflowHandler creates a thrift handler for the cadence service
func NewWorkflowHandler(
	sVice service.Service, metadataMgr persistence.MetadataManager,
	historyMgr persistence.HistoryManager, visibilityMgr persistence.VisibilityManager) (*WorkflowHandler, []thrift.TChanServer) {
	handler := &WorkflowHandler{
		Service:         sVice,
		metadataMgr:     metadataMgr,
		historyMgr:      historyMgr,
		visibitiltyMgr:  visibilityMgr,
		tokenSerializer: common.NewJSONTaskTokenSerializer(),
		hSerializer:     common.NewJSONHistorySerializer(),
		domainCache:     cache.NewDomainCache(metadataMgr, sVice.GetLogger()),
	}
	// prevent us from trying to serve requests before handler's Start() is complete
	handler.startWG.Add(1)
	return handler, []thrift.TChanServer{cadence.NewTChanWorkflowServiceServer(handler)}
}

// Start starts the handler
func (wh *WorkflowHandler) Start(thriftService []thrift.TChanServer) error {
	wh.Service.Start(thriftService)
	var err error
	wh.history, err = wh.Service.GetClientFactory().NewHistoryClient()
	if err != nil {
		return err
	}
	wh.matching, err = wh.Service.GetClientFactory().NewMatchingClient()
	if err != nil {
		return err
	}
	wh.startWG.Done()
	return nil
}

// Stop stops the handler
func (wh *WorkflowHandler) Stop() {
	wh.Service.Stop()
}

// IsHealthy - Health endpoint.
func (wh *WorkflowHandler) IsHealthy(ctx thrift.Context) (bool, error) {
	log.Println("Workflow Health endpoint reached.")
	return true, nil
}

func (wh *WorkflowHandler) RegisterDomain(ctx thrift.Context, registerRequest *gen.RegisterDomainRequest) error {
	wh.startWG.Wait()

	response, err := wh.metadataMgr.CreateDomain(&persistence.CreateDomainRequest{
		Name:        registerRequest.GetName(),
		Status:      persistence.DomainStatusRegistered,
		OwnerEmail:  registerRequest.GetOwnerEmail(),
		Description: registerRequest.GetDescription(),
		Retention:   registerRequest.GetWorkflowExecutionRetentionPeriodInDays(),
		EmitMetric:  registerRequest.GetEmitMetric(),
	})

	if err != nil {
		return wrapError(err)
	}

	// TODO: Log through logging framework.  We need to have good auditing of domain CRUD
	wh.GetLogger().Debugf("Register domain succeeded for name: %v, Id: %v", registerRequest.GetName(), response.ID)
	return nil
}

func (wh *WorkflowHandler) DescribeDomain(ctx thrift.Context,
	describeRequest *gen.DescribeDomainRequest) (*gen.DescribeDomainResponse, error) {
	wh.startWG.Wait()

	resp, err := wh.metadataMgr.GetDomain(&persistence.GetDomainRequest{
		Name: describeRequest.GetName(),
	})

	if err != nil {
		return nil, wrapError(err)
	}

	response := gen.NewDescribeDomainResponse()
	response.DomainInfo, response.Configuration = createDomainResponse(resp.Info, resp.Config)

	return response, nil
}

func (wh *WorkflowHandler) UpdateDomain(ctx thrift.Context,
	updateRequest *gen.UpdateDomainRequest) (*gen.UpdateDomainResponse, error) {
	wh.startWG.Wait()

	if !updateRequest.IsSetName() {
		return nil, errDomainNotSet
	}

	domainName := updateRequest.GetName()

	getResponse, err0 := wh.metadataMgr.GetDomain(&persistence.GetDomainRequest{
		Name: domainName,
	})

	if err0 != nil {
		return nil, wrapError(err0)
	}

	info := getResponse.Info
	config := getResponse.Config

	if updateRequest.IsSetUpdatedInfo() {
		updatedInfo := updateRequest.GetUpdatedInfo()
		if updatedInfo.IsSetDescription() {
			info.Description = updatedInfo.GetDescription()
		}
		if updatedInfo.IsSetOwnerEmail() {
			info.OwnerEmail = updatedInfo.GetOwnerEmail()
		}
	}

	if updateRequest.IsSetConfiguration() {
		updatedConfig := updateRequest.GetConfiguration()
		if updatedConfig.IsSetEmitMetric() {
			config.EmitMetric = updatedConfig.GetEmitMetric()
		}
		if updatedConfig.IsSetWorkflowExecutionRetentionPeriodInDays() {
			config.Retention = updatedConfig.GetWorkflowExecutionRetentionPeriodInDays()
		}
	}

	err := wh.metadataMgr.UpdateDomain(&persistence.UpdateDomainRequest{
		Info:   info,
		Config: config,
	})
	if err != nil {
		return nil, wrapError(err)
	}

	response := gen.NewUpdateDomainResponse()
	response.DomainInfo, response.Configuration = createDomainResponse(info, config)
	return response, nil
}

func (wh *WorkflowHandler) DeprecateDomain(ctx thrift.Context, deprecateRequest *gen.DeprecateDomainRequest) error {
	wh.startWG.Wait()

	if !deprecateRequest.IsSetName() {
		return errDomainNotSet
	}

	domainName := deprecateRequest.GetName()

	getResponse, err0 := wh.metadataMgr.GetDomain(&persistence.GetDomainRequest{
		Name: domainName,
	})

	if err0 != nil {
		return wrapError(err0)
	}

	info := getResponse.Info
	info.Status = persistence.DomainStatusDeprecated
	config := getResponse.Config

	return wh.metadataMgr.UpdateDomain(&persistence.UpdateDomainRequest{
		Info:   info,
		Config: config,
	})
}

// PollForActivityTask - Poll for an activity task.
func (wh *WorkflowHandler) PollForActivityTask(
	ctx thrift.Context,
	pollRequest *gen.PollForActivityTaskRequest) (*gen.PollForActivityTaskResponse, error) {
	wh.startWG.Wait()

	wh.Service.GetLogger().Debug("Received PollForActivityTask")
	if !pollRequest.IsSetDomain() {
		return nil, errDomainNotSet
	}

	domainName := pollRequest.GetDomain()
	info, _, err := wh.domainCache.GetDomain(domainName)
	if err != nil {
		return nil, wrapError(err)
	}

	resp, err := wh.matching.PollForActivityTask(ctx, &m.PollForActivityTaskRequest{
		DomainUUID:  common.StringPtr(info.ID),
		PollRequest: pollRequest,
	})
	if err != nil {
		wh.Service.GetLogger().Errorf(
			"PollForActivityTask failed. TaskList: %v, Error: %v", pollRequest.GetTaskList().GetName(), err)
	}
	return resp, wrapError(err)
}

// PollForDecisionTask - Poll for a decision task.
func (wh *WorkflowHandler) PollForDecisionTask(
	ctx thrift.Context,
	pollRequest *gen.PollForDecisionTaskRequest) (*gen.PollForDecisionTaskResponse, error) {
	wh.startWG.Wait()

	wh.Service.GetLogger().Debug("Received PollForDecisionTask")
	if !pollRequest.IsSetDomain() {
		return nil, errDomainNotSet
	}

	domainName := pollRequest.GetDomain()
	info, _, err := wh.domainCache.GetDomain(domainName)
	if err != nil {
		return nil, wrapError(err)
	}

	wh.Service.GetLogger().Infof("Poll for decision domain name: %v", domainName)
	wh.Service.GetLogger().Infof("Poll for decision request domainID: %v", info.ID)

	matchingResp, err := wh.matching.PollForDecisionTask(ctx, &m.PollForDecisionTaskRequest{
		DomainUUID:  common.StringPtr(info.ID),
		PollRequest: pollRequest,
	})
	if err != nil {
		wh.Service.GetLogger().Errorf(
			"PollForDecisionTask failed. TaskList: %v, Error: %v", pollRequest.GetTaskList().GetName(), err)
		return nil, wrapError(err)
	}

	history, err := wh.getHistory(info.ID, *matchingResp.GetWorkflowExecution(), matchingResp.GetStartedEventId()+1)
	if err != nil {
		return nil, wrapError(err)
	}
	return createPollForDecisionTaskResponse(matchingResp, history), nil
}

// RecordActivityTaskHeartbeat - Record Activity Task Heart beat.
func (wh *WorkflowHandler) RecordActivityTaskHeartbeat(
	ctx thrift.Context,
	heartbeatRequest *gen.RecordActivityTaskHeartbeatRequest) (*gen.RecordActivityTaskHeartbeatResponse, error) {
	wh.startWG.Wait()

	wh.Service.GetLogger().Debug("Received RecordActivityTaskHeartbeat")
	if !heartbeatRequest.IsSetTaskToken() {
		return nil, errTaskTokenNotSet
	}
	taskToken, err := wh.tokenSerializer.Deserialize(heartbeatRequest.GetTaskToken())
	if err != nil {
		return nil, wrapError(err)
	}
	if taskToken.DomainID == "" {
		return nil, errDomainNotSet
	}

	resp, err := wh.history.RecordActivityTaskHeartbeat(ctx, &h.RecordActivityTaskHeartbeatRequest{
		DomainUUID:       common.StringPtr(taskToken.DomainID),
		HeartbeatRequest: heartbeatRequest,
	})
	return resp, wrapError(err)
}

// RespondActivityTaskCompleted - response to an activity task
func (wh *WorkflowHandler) RespondActivityTaskCompleted(
	ctx thrift.Context,
	completeRequest *gen.RespondActivityTaskCompletedRequest) error {
	wh.startWG.Wait()

	if !completeRequest.IsSetTaskToken() {
		return errTaskTokenNotSet
	}
	taskToken, err := wh.tokenSerializer.Deserialize(completeRequest.GetTaskToken())
	if err != nil {
		return wrapError(err)
	}
	if taskToken.DomainID == "" {
		return errDomainNotSet
	}

	err = wh.history.RespondActivityTaskCompleted(ctx, &h.RespondActivityTaskCompletedRequest{
		DomainUUID:      common.StringPtr(taskToken.DomainID),
		CompleteRequest: completeRequest,
	})
	if err != nil {
		logger := wh.getLoggerForTask(completeRequest.GetTaskToken())
		logger.Errorf("RespondActivityTaskCompleted. Error: %v", err)
	}
	return wrapError(err)
}

// RespondActivityTaskFailed - response to an activity task failure
func (wh *WorkflowHandler) RespondActivityTaskFailed(
	ctx thrift.Context,
	failedRequest *gen.RespondActivityTaskFailedRequest) error {
	wh.startWG.Wait()

	if !failedRequest.IsSetTaskToken() {
		return errTaskTokenNotSet
	}
	taskToken, err := wh.tokenSerializer.Deserialize(failedRequest.GetTaskToken())
	if err != nil {
		return wrapError(err)
	}
	if taskToken.DomainID == "" {
		return errDomainNotSet
	}

	err = wh.history.RespondActivityTaskFailed(ctx, &h.RespondActivityTaskFailedRequest{
		DomainUUID:    common.StringPtr(taskToken.DomainID),
		FailedRequest: failedRequest,
	})
	if err != nil {
		logger := wh.getLoggerForTask(failedRequest.GetTaskToken())
		logger.Errorf("RespondActivityTaskFailed. Error: %v", err)
	}
	return wrapError(err)

}

// RespondActivityTaskCanceled - called to cancel an activity task
func (wh *WorkflowHandler) RespondActivityTaskCanceled(
	ctx thrift.Context,
	cancelRequest *gen.RespondActivityTaskCanceledRequest) error {
	wh.startWG.Wait()

	if !cancelRequest.IsSetTaskToken() {
		return errTaskTokenNotSet
	}
	taskToken, err := wh.tokenSerializer.Deserialize(cancelRequest.GetTaskToken())
	if err != nil {
		return wrapError(err)
	}
	if taskToken.DomainID == "" {
		return errDomainNotSet
	}

	err = wh.history.RespondActivityTaskCanceled(ctx, &h.RespondActivityTaskCanceledRequest{
		DomainUUID:    common.StringPtr(taskToken.DomainID),
		CancelRequest: cancelRequest,
	})
	if err != nil {
		logger := wh.getLoggerForTask(cancelRequest.GetTaskToken())
		logger.Errorf("RespondActivityTaskCanceled. Error: %v", err)
	}
	return wrapError(err)

}

// RespondDecisionTaskCompleted - response to a decision task
func (wh *WorkflowHandler) RespondDecisionTaskCompleted(
	ctx thrift.Context,
	completeRequest *gen.RespondDecisionTaskCompletedRequest) error {
	wh.startWG.Wait()

	if !completeRequest.IsSetTaskToken() {
		return errTaskTokenNotSet
	}
	taskToken, err := wh.tokenSerializer.Deserialize(completeRequest.GetTaskToken())
	if err != nil {
		return wrapError(err)
	}
	if taskToken.DomainID == "" {
		return errDomainNotSet
	}

	err = wh.history.RespondDecisionTaskCompleted(ctx, &h.RespondDecisionTaskCompletedRequest{
		DomainUUID:      common.StringPtr(taskToken.DomainID),
		CompleteRequest: completeRequest,
	})
	if err != nil {
		logger := wh.getLoggerForTask(completeRequest.GetTaskToken())
		logger.Errorf("RespondDecisionTaskCompleted. Error: %v", err)
	}
	return wrapError(err)
}

// StartWorkflowExecution - Creates a new workflow execution
func (wh *WorkflowHandler) StartWorkflowExecution(
	ctx thrift.Context,
	startRequest *gen.StartWorkflowExecutionRequest) (*gen.StartWorkflowExecutionResponse, error) {
	wh.startWG.Wait()

	wh.Service.GetLogger().Debugf("Received StartWorkflowExecution. WorkflowID: %v", startRequest.GetWorkflowId())

	if !startRequest.IsSetDomain() {
		return nil, errDomainNotSet
	}

	domainName := startRequest.GetDomain()
	wh.Service.GetLogger().Infof("Start workflow execution request domain: %v", domainName)
	info, _, err := wh.domainCache.GetDomain(domainName)
	if err != nil {
		return nil, wrapError(err)
	}

	wh.Service.GetLogger().Infof("Start workflow execution request domainID: %v", info.ID)

	resp, err := wh.history.StartWorkflowExecution(ctx, &h.StartWorkflowExecutionRequest{
		DomainUUID:   common.StringPtr(info.ID),
		StartRequest: startRequest,
	})
	if err != nil {
		wh.Service.GetLogger().Errorf("StartWorkflowExecution failed. WorkflowID: %v. Error: %v", startRequest.GetWorkflowId(), err)
	}
	return resp, wrapError(err)
}

// GetWorkflowExecutionHistory - retrieves the hisotry of workflow execution
func (wh *WorkflowHandler) GetWorkflowExecutionHistory(
	ctx thrift.Context,
	getRequest *gen.GetWorkflowExecutionHistoryRequest) (*gen.GetWorkflowExecutionHistoryResponse, error) {
	wh.startWG.Wait()

	if !getRequest.IsSetDomain() {
		return nil, errDomainNotSet
	}

	domainName := getRequest.GetDomain()
	info, _, err := wh.domainCache.GetDomain(domainName)
	if err != nil {
		return nil, wrapError(err)
	}

	response, err := wh.history.GetWorkflowExecutionHistory(ctx, &h.GetWorkflowExecutionHistoryRequest{
		DomainUUID: common.StringPtr(info.ID),
		GetRequest: getRequest,
	})
	if err != nil {
		return nil, wrapError(err)
	}

	return response, nil
}

func (wh *WorkflowHandler) SignalWorkflowExecution(ctx thrift.Context,
	signalRequest *gen.SignalWorkflowExecutionRequest) error {
	wh.startWG.Wait()

	if !signalRequest.IsSetDomain() {
		return errDomainNotSet
	}

	domainName := signalRequest.GetDomain()
	info, _, err := wh.domainCache.GetDomain(domainName)
	if err != nil {
		return wrapError(err)
	}

	err = wh.history.SignalWorkflowExecution(ctx, &h.SignalWorkflowExecutionRequest{
		DomainUUID:    common.StringPtr(info.ID),
		SignalRequest: signalRequest,
	})

	return wrapError(err)
}

func (wh *WorkflowHandler) TerminateWorkflowExecution(ctx thrift.Context,
	terminateRequest *gen.TerminateWorkflowExecutionRequest) error {
	wh.startWG.Wait()

	if !terminateRequest.IsSetDomain() {
		return errDomainNotSet
	}

	domainName := terminateRequest.GetDomain()
	info, _, err := wh.domainCache.GetDomain(domainName)
	if err != nil {
		return wrapError(err)
	}

	err = wh.history.TerminateWorkflowExecution(ctx, &h.TerminateWorkflowExecutionRequest{
		DomainUUID:       common.StringPtr(info.ID),
		TerminateRequest: terminateRequest,
	})

	return wrapError(err)
}

// ListOpenWorkflowExecutions - retrieves info for open workflow executions in a domain
func (wh *WorkflowHandler) ListOpenWorkflowExecutions(ctx thrift.Context,
	listRequest *gen.ListOpenWorkflowExecutionsRequest) (*gen.ListOpenWorkflowExecutionsResponse, error) {

	if !listRequest.IsSetDomain() {
		return nil, errDomainNotSet
	}

	if !listRequest.IsSetStartTimeFilter() {
		return nil, &gen.BadRequestError{
			Message: "StartTimeFilter is required",
		}
	}

	if !listRequest.GetStartTimeFilter().IsSetEarliestTime() {
		return nil, &gen.BadRequestError{
			Message: "EarliestTime in StartTimeFilter is required",
		}
	}

	if !listRequest.GetStartTimeFilter().IsSetLatestTime() {
		return nil, &gen.BadRequestError{
			Message: "LatestTime in StartTimeFilter is required",
		}
	}

	if listRequest.IsSetExecutionFilter() && listRequest.IsSetTypeFilter() {
		return nil, &gen.BadRequestError{
			Message: "Only one of ExecutionFilter or TypeFilter is allowed",
		}
	}

	domainName := listRequest.GetDomain()
	domainInfo, _, err := wh.domainCache.GetDomain(domainName)
	if err != nil {
		return nil, wrapError(err)
	}

	baseReq := persistence.ListWorkflowExecutionsRequest{
		DomainUUID:        domainInfo.ID,
		PageSize:          int(listRequest.GetMaximumPageSize()),
		NextPageToken:     listRequest.GetNextPageToken(),
		EarliestStartTime: listRequest.GetStartTimeFilter().GetEarliestTime(),
		LatestStartTime:   listRequest.GetStartTimeFilter().GetLatestTime(),
	}

	var persistenceResp *persistence.ListWorkflowExecutionsResponse
	if listRequest.IsSetExecutionFilter() {
		persistenceResp, err = wh.visibitiltyMgr.ListOpenWorkflowExecutionsByWorkflowID(
			&persistence.ListWorkflowExecutionsByWorkflowIDRequest{
				ListWorkflowExecutionsRequest: baseReq,
				WorkflowID:                    listRequest.ExecutionFilter.GetWorkflowId(),
			})
	} else if listRequest.IsSetTypeFilter() {
		persistenceResp, err = wh.visibitiltyMgr.ListOpenWorkflowExecutionsByType(&persistence.ListWorkflowExecutionsByTypeRequest{
			ListWorkflowExecutionsRequest: baseReq,
			WorkflowTypeName:              listRequest.TypeFilter.GetName(),
		})
	} else {
		persistenceResp, err = wh.visibitiltyMgr.ListOpenWorkflowExecutions(&baseReq)
	}

	if err != nil {
		return nil, wrapError(err)
	}

	resp := gen.NewListOpenWorkflowExecutionsResponse()
	resp.Executions = persistenceResp.Executions
	resp.NextPageToken = persistenceResp.NextPageToken
	return resp, nil
}

// ListClosedWorkflowExecutions - retrieves info for closed workflow executions in a domain
func (wh *WorkflowHandler) ListClosedWorkflowExecutions(ctx thrift.Context,
	listRequest *gen.ListClosedWorkflowExecutionsRequest) (*gen.ListClosedWorkflowExecutionsResponse, error) {
	if !listRequest.IsSetDomain() {
		return nil, errDomainNotSet
	}

	if !listRequest.IsSetStartTimeFilter() {
		return nil, &gen.BadRequestError{
			Message: "StartTimeFilter is required",
		}
	}

	if !listRequest.GetStartTimeFilter().IsSetEarliestTime() {
		return nil, &gen.BadRequestError{
			Message: "EarliestTime in StartTimeFilter is required",
		}
	}

	if !listRequest.GetStartTimeFilter().IsSetLatestTime() {
		return nil, &gen.BadRequestError{
			Message: "LatestTime in StartTimeFilter is required",
		}
	}

	if listRequest.IsSetExecutionFilter() && listRequest.IsSetTypeFilter() {
		return nil, &gen.BadRequestError{
			Message: "Only one of ExecutionFilter or TypeFilter is allowed",
		}
	}

	domainName := listRequest.GetDomain()
	domainInfo, _, err := wh.domainCache.GetDomain(domainName)
	if err != nil {
		return nil, wrapError(err)
	}

	baseReq := persistence.ListWorkflowExecutionsRequest{
		DomainUUID:        domainInfo.ID,
		PageSize:          int(listRequest.GetMaximumPageSize()),
		NextPageToken:     listRequest.GetNextPageToken(),
		EarliestStartTime: listRequest.GetStartTimeFilter().GetEarliestTime(),
		LatestStartTime:   listRequest.GetStartTimeFilter().GetLatestTime(),
	}

	var persistenceResp *persistence.ListWorkflowExecutionsResponse
	if listRequest.IsSetExecutionFilter() {
		persistenceResp, err = wh.visibitiltyMgr.ListClosedWorkflowExecutionsByWorkflowID(
			&persistence.ListWorkflowExecutionsByWorkflowIDRequest{
				ListWorkflowExecutionsRequest: baseReq,
				WorkflowID:                    listRequest.ExecutionFilter.GetWorkflowId(),
			})
	} else if listRequest.IsSetTypeFilter() {
		persistenceResp, err = wh.visibitiltyMgr.ListClosedWorkflowExecutionsByType(&persistence.ListWorkflowExecutionsByTypeRequest{
			ListWorkflowExecutionsRequest: baseReq,
			WorkflowTypeName:              listRequest.TypeFilter.GetName(),
		})
	} else {
		persistenceResp, err = wh.visibitiltyMgr.ListClosedWorkflowExecutions(&baseReq)
	}

	if err != nil {
		return nil, wrapError(err)
	}

	resp := gen.NewListClosedWorkflowExecutionsResponse()
	resp.Executions = persistenceResp.Executions
	resp.NextPageToken = persistenceResp.NextPageToken
	return resp, nil
}

func (wh *WorkflowHandler) getHistory(
	domainID string, execution gen.WorkflowExecution, nextEventID int64) (*gen.History, error) {

	nextPageToken := []byte{}
	historyEvents := []*gen.HistoryEvent{}
Pagination_Loop:
	for {
		response, err := wh.historyMgr.GetWorkflowExecutionHistory(&persistence.GetWorkflowExecutionHistoryRequest{
			DomainID:      domainID,
			Execution:     execution,
			NextEventID:   nextEventID,
			PageSize:      100,
			NextPageToken: nextPageToken,
		})

		if err != nil {
			return nil, err
		}

		for _, data := range response.Events {
			events, _ := wh.hSerializer.Deserialize(data)
			historyEvents = append(historyEvents, events...)
		}

		if len(response.NextPageToken) == 0 {
			break Pagination_Loop
		}

		nextPageToken = response.NextPageToken
	}

	executionHistory := gen.NewHistory()
	executionHistory.Events = historyEvents
	return executionHistory, nil
}

func (wh *WorkflowHandler) getLoggerForTask(taskToken []byte) bark.Logger {
	logger := wh.Service.GetLogger()
	task, err := wh.tokenSerializer.Deserialize(taskToken)
	if err == nil {
		logger = logger.WithFields(bark.Fields{
			"WorkflowID": task.WorkflowID,
			"RunID":      task.RunID,
			"ScheduleID": task.ScheduleID,
		})
	}
	return logger
}

func wrapError(err error) error {
	if err != nil && shouldWrapInInternalServiceError(err) {
		return &gen.InternalServiceError{Message: err.Error()}
	}
	return err
}

func shouldWrapInInternalServiceError(err error) bool {
	switch err.(type) {
	case *gen.InternalServiceError:
		return false
	case *gen.BadRequestError:
		return false
	case *gen.EntityNotExistsError:
		return false
	case *gen.WorkflowExecutionAlreadyStartedError:
		return false
	case *gen.DomainAlreadyExistsError:
		return false
	}

	return true
}

func getDomainStatus(info *persistence.DomainInfo) *gen.DomainStatus {
	switch info.Status {
	case persistence.DomainStatusRegistered:
		return gen.DomainStatusPtr(gen.DomainStatus_REGISTERED)
	case persistence.DomainStatusDeprecated:
		return gen.DomainStatusPtr(gen.DomainStatus_DEPRECATED)
	case persistence.DomainStatusDeleted:
		return gen.DomainStatusPtr(gen.DomainStatus_DELETED)
	}

	return nil
}

func createDomainResponse(info *persistence.DomainInfo, config *persistence.DomainConfig) (*gen.DomainInfo,
	*gen.DomainConfiguration) {

	i := gen.NewDomainInfo()
	i.Name = common.StringPtr(info.Name)
	i.Status = getDomainStatus(info)
	i.Description = common.StringPtr(info.Description)
	i.OwnerEmail = common.StringPtr(info.OwnerEmail)

	c := gen.NewDomainConfiguration()
	c.EmitMetric = common.BoolPtr(config.EmitMetric)
	c.WorkflowExecutionRetentionPeriodInDays = common.Int32Ptr(config.Retention)

	return i, c
}

func createPollForDecisionTaskResponse(
	matchingResponse *m.PollForDecisionTaskResponse, history *gen.History) *gen.PollForDecisionTaskResponse {
	resp := gen.NewPollForDecisionTaskResponse()
	resp.TaskToken = matchingResponse.TaskToken
	resp.WorkflowExecution = matchingResponse.WorkflowExecution
	resp.WorkflowType = matchingResponse.WorkflowType
	resp.PreviousStartedEventId = matchingResponse.PreviousStartedEventId
	resp.StartedEventId = matchingResponse.StartedEventId
	resp.History = history
	return resp
}
