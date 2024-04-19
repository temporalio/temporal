// The MIT License
//
// Copyright (c) 2023 Temporal Technologies Inc.  All rights reserved.
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

package nexus

import (
	"errors"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ProtoFailureToNexusFailure converts a proto Nexus Failure to a Nexus SDK Failure.
// Always returns a non-nil value.
func ProtoFailureToNexusFailure(failure *nexuspb.Failure) *nexus.Failure {
	return &nexus.Failure{
		Message:  failure.GetMessage(),
		Metadata: failure.GetMetadata(),
		Details:  failure.GetDetails(),
	}
}

// NexusFailureToProtoFailure converts a Nexus SDK Failure to a proto Nexus Failure.
// Always returns a non-nil value.
func NexusFailureToProtoFailure(failure *nexus.Failure) *nexuspb.Failure {
	if failure == nil {
		return &nexuspb.Failure{Message: "unknown error"}
	}
	return &nexuspb.Failure{
		Message:  failure.Message,
		Metadata: failure.Metadata,
		Details:  failure.Details,
	}
}

// APIFailureToNexusFailure converts an API proto Failure to a Nexus SDK Failure taking only the failure message to
// avoid leaking too many details to 3rd party callers.
// Always returns a non-nil value.
func APIFailureToNexusFailure(failure *failurepb.Failure) *nexus.Failure {
	return &nexus.Failure{
		Message: failure.GetMessage(),
	}
}

func UnsuccessfulOperationErrorToTemporalFailure(err *nexus.UnsuccessfulOperationError) *failurepb.Failure {
	failure := &failurepb.Failure{
		Message: err.Failure.Message,
	}
	if err.State == nexus.OperationStateCanceled {
		failure.FailureInfo = &failurepb.Failure_CanceledFailureInfo{
			CanceledFailureInfo: &failurepb.CanceledFailureInfo{
				Details: nexusFailureMetadataToPayloads(err.Failure),
			},
		}
	} else {
		failure.FailureInfo = &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
				// Make up a type here, it's not part of the Nexus Failure spec.
				Type:         "NexusOperationFailure",
				Details:      nexusFailureMetadataToPayloads(err.Failure),
				NonRetryable: true,
			},
		}
	}
	return failure
}

func nexusFailureMetadataToPayloads(failure nexus.Failure) *commonpb.Payloads {
	if len(failure.Metadata) == 0 && len(failure.Details) == 0 {
		return nil
	}
	metadata := make(map[string][]byte, len(failure.Metadata))
	for k, v := range failure.Metadata {
		metadata[k] = []byte(v)
	}
	return &commonpb.Payloads{
		Payloads: []*commonpb.Payload{
			{
				Metadata: metadata,
				Data:     failure.Details,
			},
		},
	}
}

// ConvertGRPCError converts either a serviceerror or a gRPC status error into a Nexus HandlerError if possible.
// If exposeDetails is true, the error message from the given error is exposed in the converted HandlerError, otherwise,
// a default message with minimal information is attached to the returned error.
// Roughly taken from https://github.com/googleapis/googleapis/blob/master/google/rpc/code.proto
// and
// https://github.com/grpc-ecosystem/grpc-gateway/blob/a7cf811e6ffabeaddcfb4ff65602c12671ff326e/runtime/errors.go#L56.
func ConvertGRPCError(err error, exposeDetails bool) error {
	var st *status.Status
	stGetter, ok := err.(interface{ Status() *status.Status })
	if ok {
		st = stGetter.Status()
	} else {
		st, ok = status.FromError(err)
		if !ok {
			// The Nexus SDK will translate this into an internal server error and will not expose the error details.
			return err
		}
	}

	errMessage := err.Error()

	switch st.Code() {
	case codes.AlreadyExists, codes.Canceled, codes.InvalidArgument, codes.FailedPrecondition, codes.OutOfRange:
		if !exposeDetails {
			errMessage = "bad request"
		}
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, errMessage)
	case codes.Aborted, codes.Unavailable:
		if !exposeDetails {
			errMessage = "service unavailable"
		}
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeUnavailable, errMessage)
	case codes.DataLoss, codes.Internal, codes.Unknown:
		if !exposeDetails {
			errMessage = "internal error"
		}
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeInternal, errMessage)
	case codes.Unauthenticated:
		if !exposeDetails {
			errMessage = "authentication failed"
		}
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeUnauthenticated, errMessage)
	case codes.PermissionDenied:
		if !exposeDetails {
			errMessage = "permission denied"
		}
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeUnauthorized, errMessage)
	case codes.NotFound:
		if !exposeDetails {
			errMessage = "not found"
		}
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeNotFound, errMessage)
	case codes.ResourceExhausted:
		if !exposeDetails {
			errMessage = "resource exhausted"
		}
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeResourceExhausted, errMessage)
	case codes.Unimplemented:
		if !exposeDetails {
			errMessage = "not implemented"
		}
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeNotImplemented, errMessage)
	case codes.DeadlineExceeded:
		if !exposeDetails {
			errMessage = "request timeout"
		}
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeDownstreamTimeout, errMessage)
	case codes.OK:
		return nil
	}
	if !exposeDetails {
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeInternal, "internal error")
	}
	// Let the nexus SDK handle this for us (log and convert to an internal error).
	return err
}

func AdaptAuthorizeError(err error) error {
	// Authorize err is either an explicitly set reason, or a generic "Request unauthorized." message.
	var permissionDeniedError *serviceerror.PermissionDenied
	if errors.As(err, &permissionDeniedError) && permissionDeniedError.Reason != "" {
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeUnauthorized, "permission denied: %s", permissionDeniedError.Reason)
	}
	return nexus.HandlerErrorf(nexus.HandlerErrorTypeUnauthorized, "permission denied")
}
