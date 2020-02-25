// Copyright (c) 2019 Temporal Technologies, Inc.
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

package adapter

import (
	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/.gen/go/history"
	"github.com/temporalio/temporal/.gen/go/shared"
)

// ToProtoError converts Thrift error to gRPC error.
func ToServiceError(in error) error {
	if in == nil {
		return nil
	}

	switch thriftError := in.(type) {
	case *shared.InternalServiceError:
		return serviceerror.NewInternal(thriftError.Message)
	case *shared.BadRequestError:
		switch thriftError.Message {
		case "No permission to do this operation.":
			return serviceerror.NewPermissionDenied(thriftError.Message)
		default:
			return serviceerror.NewInvalidArgument(thriftError.Message)
		}
	case *shared.DomainNotActiveError:
		return serviceerror.NewDomainNotActive(thriftError.Message, thriftError.DomainName, thriftError.CurrentCluster, thriftError.ActiveCluster)
	case *shared.EntityNotExistsError:
		return serviceerror.NewNotFound(thriftError.Message)
	case *shared.WorkflowExecutionAlreadyStartedError:
		return serviceerror.NewWorkflowExecutionAlreadyStarted(*thriftError.Message, *thriftError.StartRequestId, *thriftError.RunId)
	case *shared.DomainAlreadyExistsError:
		return serviceerror.NewDomainAlreadyExists(thriftError.Message)
	case *shared.CancellationAlreadyRequestedError:
		return serviceerror.NewCancellationAlreadyRequested(thriftError.Message)
	case *shared.QueryFailedError:
		return serviceerror.NewQueryFailed(thriftError.Message)
	case *shared.LimitExceededError:
		return serviceerror.NewResourceExhausted(thriftError.Message)
	case *shared.ServiceBusyError:
		return serviceerror.NewResourceExhausted(thriftError.Message)
	case *shared.ClientVersionNotSupportedError:
		return serviceerror.NewClientVersionNotSupported("Client version is not supported.", thriftError.FeatureVersion, thriftError.ClientImpl, thriftError.SupportedVersions)
	case *history.ShardOwnershipLostError:
		return serviceerror.NewShardOwnershipLost(*thriftError.Message, *thriftError.Owner)
	case *history.EventAlreadyStartedError:
		return serviceerror.NewEventAlreadyStarted(thriftError.Message)
	case *shared.RetryTaskError:
		return serviceerror.NewRetryTask(thriftError.Message, *thriftError.DomainId, *thriftError.WorkflowId, *thriftError.RunId, *thriftError.NextEventId)
	case *shared.RetryTaskV2Error:
		return serviceerror.NewRetryTaskV2(thriftError.Message, *thriftError.DomainId, *thriftError.WorkflowId, *thriftError.RunId, *thriftError.StartEventId, *thriftError.StartEventVersion, *thriftError.EndEventId, *thriftError.EndEventVersion)
	case *shared.InternalDataInconsistencyError:
		return serviceerror.NewDataLoss(thriftError.Message)
	case *shared.AccessDeniedError:
		return serviceerror.NewPermissionDenied(thriftError.Message)
	case *shared.CurrentBranchChangedError:
		return serviceerror.NewCurrentBranchChanged(thriftError.Message, thriftError.CurrentBranchToken)
	}

	return in
}
