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
	"fmt"

	"github.com/gogo/status"
	"go.temporal.io/temporal-proto/errordetails"
	"go.uber.org/yarpc/encoding/protobuf"
	"go.uber.org/yarpc/yarpcerrors"
	"google.golang.org/grpc/codes"

	"github.com/temporalio/temporal/.gen/go/history"
	"github.com/temporalio/temporal/.gen/go/shared"
)

// ToProtoError converts Thrift error to gRPC error.
func ToProtoError(in error) error {
	if in == nil {
		return nil
	}

	if _, ok := status.FromError(in); ok {
		return in
	}

	var st *status.Status
	switch thriftError := in.(type) {
	case *shared.InternalServiceError:
		st = status.New(codes.Internal, thriftError.Message)
	case *shared.BadRequestError:
		switch thriftError.Message {
		case "No permission to do this operation.":
			st = status.New(codes.PermissionDenied, thriftError.Message)
		default:
			st = status.New(codes.InvalidArgument, thriftError.Message)
		}
	case *shared.DomainNotActiveError:
		st = errordetails.NewDomainNotActiveStatus(thriftError.Message, thriftError.DomainName, thriftError.CurrentCluster, thriftError.ActiveCluster)
	case *shared.ServiceBusyError:
		st = status.New(codes.ResourceExhausted, thriftError.Message)
	case *shared.EntityNotExistsError:
		st = status.New(codes.NotFound, thriftError.Message)
	case *shared.WorkflowExecutionAlreadyStartedError:
		st = errordetails.NewWorkflowExecutionAlreadyStartedStatus(*thriftError.Message, *thriftError.StartRequestId, *thriftError.RunId)
	case *shared.DomainAlreadyExistsError:
		st = status.New(codes.AlreadyExists, thriftError.Message)
	case *shared.CancellationAlreadyRequestedError:
		st = status.New(codes.AlreadyExists, thriftError.Message)
	case *shared.QueryFailedError:
		st = status.New(codes.InvalidArgument, thriftError.Message)
	case *shared.LimitExceededError:
		st = status.New(codes.ResourceExhausted, thriftError.Message)
	case *shared.ClientVersionNotSupportedError:
		st = errordetails.NewClientVersionNotSupportedStatus("Client version is not supported.", thriftError.FeatureVersion, thriftError.ClientImpl, thriftError.SupportedVersions)
	case *history.ShardOwnershipLostError:
		st = errordetails.NewShardOwnershipLostStatus(*thriftError.Message, *thriftError.Owner)
	case *history.EventAlreadyStartedError:
		st = status.New(codes.AlreadyExists, thriftError.Message)
	case *shared.RetryTaskError:
		st = errordetails.NewRetryTaskStatus(thriftError.Message, *thriftError.DomainId, *thriftError.WorkflowId, *thriftError.RunId, *thriftError.NextEventId)
	case *shared.RetryTaskV2Error:
		st = errordetails.NewRetryTaskV2Status(thriftError.Message, *thriftError.DomainId, *thriftError.WorkflowId, *thriftError.RunId, *thriftError.StartEventId, *thriftError.StartEventVersion, *thriftError.EndEventId, *thriftError.EndEventVersion)
	case *yarpcerrors.Status:
		if thriftError.Code() == yarpcerrors.CodeDeadlineExceeded {
			st = status.New(codes.DeadlineExceeded, thriftError.Message())
		}
	}

	if st == nil {
		st = status.New(codes.Internal, fmt.Sprintf("temporal internal uncategorized error, msg: %s", in.Error()))
	}

	return st.Err()
}

// ToThriftError converts gRPC error to Thrift error.
func ToThriftError(st *status.Status) error {
	if st == nil {
		return nil
	}

	switch st.Code() {
	case codes.Internal:
		return &shared.InternalServiceError{Message: st.Message()}
	case codes.InvalidArgument:
		if f, ok := errordetails.GetDomainNotActiveFailure(st); ok {
			return &shared.DomainNotActiveError{Message: st.Message(), DomainName: f.DomainName, ActiveCluster: f.ActiveCluster, CurrentCluster: f.CurrentCluster}
		}
		return &shared.BadRequestError{Message: st.Message()}
	case codes.PermissionDenied:
		return &shared.BadRequestError{Message: "No permission to do this operation."}
	case codes.ResourceExhausted:
		return &shared.ServiceBusyError{Message: st.Message()}
	case codes.NotFound:
		return &shared.EntityNotExistsError{Message: st.Message()}
	case codes.AlreadyExists:
		if f, ok := errordetails.GetWorkflowExecutionAlreadyStartedFailure(st); ok {
			message := st.Message()
			return &shared.WorkflowExecutionAlreadyStartedError{
				Message:        &message,
				StartRequestId: &f.StartRequestId,
				RunId:          &f.RunId,
			}
		}
		if st.Message() == "Domain already exists." {
			return &shared.DomainAlreadyExistsError{Message: st.Message()}
		}

		// TODO: history.EventAlreadyStartedError
		return &shared.CancellationAlreadyRequestedError{Message: st.Message()}
	case codes.FailedPrecondition:
		if f, ok := errordetails.GetClientVersionNotSupportedFailure(st); ok {
			return &shared.ClientVersionNotSupportedError{
				FeatureVersion:    f.FeatureVersion,
				ClientImpl:        f.ClientImpl,
				SupportedVersions: f.SupportedVersions,
			}
		}
	case codes.Aborted:
		if f, ok := errordetails.GetShardOwnershipLostFailure(st); ok {
			message := st.Message()
			return &history.ShardOwnershipLostError{
				Message: &message,
				Owner:   &f.Owner,
			}
		}
		if f, ok := errordetails.GetRetryTaskFailure(st); ok {
			message := st.Message()
			return &shared.RetryTaskError{
				Message:     message,
				DomainId:    &f.DomainId,
				WorkflowId:  &f.WorkflowId,
				RunId:       &f.RunId,
				NextEventId: &f.NextEventId,
			}
		}
		if f, ok := errordetails.GetRetryTaskV2Failure(st); ok {
			message := st.Message()
			return &shared.RetryTaskV2Error{
				Message:           message,
				DomainId:          &f.DomainId,
				WorkflowId:        &f.WorkflowId,
				RunId:             &f.RunId,
				StartEventId:      &f.StartEventId,
				StartEventVersion: &f.StartEventVersion,
				EndEventId:        &f.EndEventId,
				EndEventVersion:   &f.EndEventVersion,
			}
		}
	case codes.DeadlineExceeded:
		return protobuf.NewError(yarpcerrors.CodeDeadlineExceeded, st.Message())
	}

	return &shared.InternalServiceError{Message: fmt.Sprintf("temporal internal uncategorized error, msg: %s", st.Message())}
}
