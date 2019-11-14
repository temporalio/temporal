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

	"go.uber.org/yarpc/encoding/protobuf"
	"go.uber.org/yarpc/yarpcerrors"

	"github.com/temporalio/temporal-proto/errordetails"
	"github.com/temporalio/temporal/.gen/go/shared"
)

// ToProtoError converts Thrift error to gRPC error.
func ToProtoError(in error) error {
	switch thriftError := in.(type) {
	case *shared.InternalServiceError:
		return protobuf.NewError(yarpcerrors.CodeInternal, thriftError.Message)
	case *shared.BadRequestError:
		switch thriftError.Message {
		case "No permission to do this operation.":
			return protobuf.NewError(yarpcerrors.CodePermissionDenied, thriftError.Message)
		case "Requested workflow history has passed retention period.":
			return protobuf.NewError(yarpcerrors.CodeDeadlineExceeded, thriftError.Message)
		default:
			return protobuf.NewError(yarpcerrors.CodeInvalidArgument, thriftError.Message)
		}
	case *shared.DomainNotActiveError:
		return protobuf.NewError(yarpcerrors.CodeInvalidArgument, thriftError.Message, protobuf.WithErrorDetails(
			&errordetails.DomainNotActiveFailure{
				Message:        thriftError.Message,
				DomainName:     thriftError.DomainName,
				CurrentCluster: thriftError.CurrentCluster,
				ActiveCluster:  thriftError.ActiveCluster,
			},
		))
	case *shared.ServiceBusyError:
		return protobuf.NewError(yarpcerrors.CodeResourceExhausted, thriftError.Message)
	case *shared.EntityNotExistsError:
		return protobuf.NewError(yarpcerrors.CodeNotFound, thriftError.Message)
	case *shared.WorkflowExecutionAlreadyStartedError:
		return protobuf.NewError(yarpcerrors.CodeAlreadyExists, *thriftError.Message, protobuf.WithErrorDetails(
			&errordetails.WorkflowExecutionAlreadyStartedFailure{
				Message:        *thriftError.Message,
				StartRequestId: *thriftError.StartRequestId,
				RunId:          *thriftError.RunId,
			},
		))
	case *shared.DomainAlreadyExistsError:
		return protobuf.NewError(yarpcerrors.CodeAlreadyExists, thriftError.Message)
	case *shared.CancellationAlreadyRequestedError:
		return protobuf.NewError(yarpcerrors.CodeAlreadyExists, thriftError.Message)
	case *shared.QueryFailedError:
		return protobuf.NewError(yarpcerrors.CodeInternal, thriftError.Message)
	case *shared.LimitExceededError:
		return protobuf.NewError(yarpcerrors.CodeResourceExhausted, thriftError.Message)
	case *shared.ClientVersionNotSupportedError:
		return protobuf.NewError(yarpcerrors.CodeFailedPrecondition, "Client version is not supported.", protobuf.WithErrorDetails(
			&errordetails.ClientVersionNotSupportedFailure{
				FeatureVersion: thriftError.FeatureVersion, ClientImpl: thriftError.ClientImpl, SupportedVersions: thriftError.SupportedVersions,
			},
		))
	case *yarpcerrors.Status:
		if thriftError.Code() == yarpcerrors.CodeDeadlineExceeded {
			return protobuf.NewError(yarpcerrors.CodeDeadlineExceeded, thriftError.Message())
		}
	}

	return protobuf.NewError(yarpcerrors.CodeInternal, fmt.Sprintf("temporal internal uncategorized error, msg: %s", in.Error()))
}
