// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

namespace java com.uber.cadence.admin

include "shared.thrift"

/**
* AdminService provides advanced APIs for debugging and analysis with admin privillege
**/
service AdminService {
  /**
  * DescribeWorkflowExecution returns information about the internal states of workflow execution.
  **/
  DescribeWorkflowExecutionResponse DescribeWorkflowExecution(1: DescribeWorkflowExecutionRequest request)
    throws (
      1: shared.BadRequestError         badRequestError,
      2: shared.InternalServiceError    internalServiceError,
      3: shared.EntityNotExistsError    entityNotExistError,
      4: shared.AccessDeniedError       accessDeniedError,
    )

  /**
  * DescribeHistoryHost returns information about the internal states of a history host
  **/
  shared.DescribeHistoryHostResponse DescribeHistoryHost(1: shared.DescribeHistoryHostRequest request)
    throws (
      1: shared.BadRequestError       badRequestError,
      2: shared.InternalServiceError  internalServiceError,
      3: shared.AccessDeniedError     accessDeniedError,
    )

  /**
  * Returns the raw history of specified workflow execution.  It fails with 'EntityNotExistError' if speficied workflow
  * execution in unknown to the service.
  **/
  GetWorkflowExecutionRawHistoryResponse GetWorkflowExecutionRawHistory(1: GetWorkflowExecutionRawHistoryRequest getRequest)
    throws (
      1: shared.BadRequestError badRequestError,
      2: shared.InternalServiceError internalServiceError,
      3: shared.EntityNotExistsError entityNotExistError,
      4: shared.ServiceBusyError serviceBusyError,
    )
}

struct DescribeWorkflowExecutionRequest {
  10: optional string                       domain
  20: optional shared.WorkflowExecution     execution
}

struct DescribeWorkflowExecutionResponse{
  10: optional string shardId
  20: optional string historyAddr
  40: optional string mutableStateInCache
  50: optional string mutableStateInDatabase
}

struct GetWorkflowExecutionRawHistoryRequest {
  10: optional string domain
  20: optional shared.WorkflowExecution execution
  30: optional i64 (js.type = "Long") firstEventId
  40: optional i64 (js.type = "Long") nextEventId
  50: optional i32 maximumPageSize
  60: optional binary nextPageToken
}

struct GetWorkflowExecutionRawHistoryResponse {
  10: optional binary nextPageToken
  20: optional list<shared.DataBlob> historyBatches
  30: optional map<string, shared.ReplicationInfo> replicationInfo
  40: optional i32 eventStoreVersion
  50: optional i64 (js.type = "Long") createTaskId
}