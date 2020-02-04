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
	"go.uber.org/yarpc/yarpcerrors"
	"google.golang.org/grpc/codes"

	"github.com/temporalio/temporal/.gen/go/shared"
)

// ToProtoError converts Thrift error to gRPC error.
func ToProtoError(in error) error {
	if in == nil {
		return nil
	}

	var st *status.Status
	switch thriftError := in.(type) {
	case *shared.InternalServiceError:
		st = status.New(codes.Internal, thriftError.Message)
	case *shared.BadRequestError:
		switch thriftError.Message {
		case "No permission to do this operation.":
			st = status.New(codes.PermissionDenied, thriftError.Message)
		case "Requested workflow history has passed retention period.":
			st = status.New(codes.DeadlineExceeded, thriftError.Message)
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
