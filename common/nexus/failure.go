package nexus

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync/atomic"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	// FailureSourceHeaderName is the header used to indicate from where the Nexus failure originated.
	FailureSourceHeaderName = "Temporal-Nexus-Failure-Source"
	// FailureSourceWorker indicates the failure originated from outside the server (e.g. bad request or on the Nexus worker).
	FailureSourceWorker = "worker"
)

type failureSourceContextKeyType struct{}

var FailureSourceContextKey = failureSourceContextKeyType{}

func SetFailureSourceOnContext(ctx context.Context, response *http.Response) {
	if response == nil || response.Header == nil {
		return
	}

	failureSourceHeader := response.Header.Get(FailureSourceHeaderName)
	if failureSourceHeader == "" {
		return
	}

	failureSourceContext := ctx.Value(FailureSourceContextKey)
	if failureSourceContext == nil {
		return
	}

	if val, ok := failureSourceContext.(*atomic.Value); ok {
		val.Store(failureSourceHeader)
	}
}

var failureTypeString = string((&failurepb.Failure{}).ProtoReflect().Descriptor().FullName())

// ProtoFailureToNexusFailure converts a proto Nexus Failure to a Nexus SDK Failure.
func ProtoFailureToNexusFailure(failure *nexuspb.Failure) nexus.Failure {
	return nexus.Failure{
		Message:  failure.GetMessage(),
		Metadata: failure.GetMetadata(),
		Details:  failure.GetDetails(),
	}
}

// NexusFailureToProtoFailure converts a Nexus SDK Failure to a proto Nexus Failure.
// Always returns a non-nil value.
func NexusFailureToProtoFailure(failure nexus.Failure) *nexuspb.Failure {
	return &nexuspb.Failure{
		Message:  failure.Message,
		Metadata: failure.Metadata,
		Details:  failure.Details,
	}
}

// APIFailureToNexusFailure converts an API proto Failure to a Nexus SDK Failure setting the metadata "type" field to
// the proto fullname of the temporal API Failure message.
// Mutates the failure temporarily, unsetting the Message field to avoid duplicating the information in the serialized
// failure. Mutating was chosen over cloning for performance reasons since this function may be called frequently.
func APIFailureToNexusFailure(failure *failurepb.Failure) (nexus.Failure, error) {
	// Unset message so it's not serialized in the details.
	var message string
	message, failure.Message = failure.Message, ""
	data, err := protojson.Marshal(failure)
	failure.Message = message

	if err != nil {
		return nexus.Failure{}, err
	}
	return nexus.Failure{
		Message: failure.GetMessage(),
		Metadata: map[string]string{
			"type": failureTypeString,
		},
		Details: data,
	}, nil
}

// NexusFailureToAPIFailure converts a Nexus Failure to an API proto Failure.
// If the failure metadata "type" field is set to the fullname of the temporal API Failure message, the failure is
// reconstructed using protojson.Unmarshal on the failure details field.
func NexusFailureToAPIFailure(failure nexus.Failure, retryable bool) (*failurepb.Failure, error) {
	apiFailure := &failurepb.Failure{}

	if failure.Metadata != nil && failure.Metadata["type"] == failureTypeString {
		if err := protojson.Unmarshal(failure.Details, apiFailure); err != nil {
			return nil, err
		}
	} else {
		payloads, err := nexusFailureMetadataToPayloads(failure)
		if err != nil {
			return nil, err
		}
		apiFailure.FailureInfo = &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
				// Make up a type here, it's not part of the Nexus Failure spec.
				Type:         "NexusFailure",
				Details:      payloads,
				NonRetryable: !retryable,
			},
		}
	}
	// Ensure this always gets written.
	apiFailure.Message = failure.Message
	return apiFailure, nil
}

func OperationErrorToTemporalFailure(opErr *nexus.OperationError) (*failurepb.Failure, error) {
	var nexusFailure nexus.Failure
	failureErr, ok := opErr.Cause.(*nexus.FailureError)
	if ok {
		nexusFailure = failureErr.Failure
	} else if opErr.Cause != nil {
		nexusFailure = nexus.Failure{Message: opErr.Cause.Error()}
	}

	// Canceled must be translated into a CanceledFailure to match the SDK expectation.
	if opErr.State == nexus.OperationStateCanceled {
		if nexusFailure.Metadata != nil && nexusFailure.Metadata["type"] == failureTypeString {
			temporalFailure, err := NexusFailureToAPIFailure(nexusFailure, false)
			if err != nil {
				return nil, err
			}
			if temporalFailure.GetCanceledFailureInfo() != nil {
				// We already have a CanceledFailure, use it.
				return temporalFailure, nil
			}
			// Fallback to encoding the Nexus failure into a Temporal canceled failure, we expect operations that end up
			// as canceled to have a CanceledFailureInfo object.
		}
		payloads, err := nexusFailureMetadataToPayloads(nexusFailure)
		if err != nil {
			return nil, err
		}
		return &failurepb.Failure{
			Message: nexusFailure.Message,
			FailureInfo: &failurepb.Failure_CanceledFailureInfo{
				CanceledFailureInfo: &failurepb.CanceledFailureInfo{
					Details: payloads,
				},
			},
		}, nil
	}

	return NexusFailureToAPIFailure(nexusFailure, false)
}

func nexusFailureMetadataToPayloads(failure nexus.Failure) (*commonpb.Payloads, error) {
	if len(failure.Metadata) == 0 && len(failure.Details) == 0 {
		return nil, nil
	}
	// Delete before serializing.
	failure.Message = ""
	data, err := json.Marshal(failure)
	if err != nil {
		return nil, err
	}
	return &commonpb.Payloads{
		Payloads: []*commonpb.Payload{
			{
				Metadata: map[string][]byte{
					"encoding": []byte("json/plain"),
				},
				Data: data,
			},
		},
	}, err
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
	case codes.AlreadyExists, codes.InvalidArgument, codes.FailedPrecondition, codes.OutOfRange:
		if !exposeDetails {
			errMessage = "bad request"
		}
		return &nexus.HandlerError{
			Type:  nexus.HandlerErrorTypeBadRequest,
			Cause: errors.New(errMessage),
		}
	case codes.Aborted, codes.Unavailable:
		if !exposeDetails {
			errMessage = "service unavailable"
		}
		return &nexus.HandlerError{
			Type:  nexus.HandlerErrorTypeUnavailable,
			Cause: errors.New(errMessage),
		}
	case codes.Canceled:
		// TODO: This should have a different status code (e.g. 499 which is semi standard but not supported by nexus).
		// The important thing is that the request is retryable, internal serves that purpose.
		if !exposeDetails {
			errMessage = "canceled"
		}
		return &nexus.HandlerError{
			Type:  nexus.HandlerErrorTypeInternal,
			Cause: errors.New(errMessage),
		}
	case codes.DataLoss, codes.Internal, codes.Unknown:
		if !exposeDetails {
			errMessage = "internal error"
		}
		return &nexus.HandlerError{
			Type:  nexus.HandlerErrorTypeInternal,
			Cause: errors.New(errMessage),
		}
	case codes.Unauthenticated:
		if !exposeDetails {
			errMessage = "authentication failed"
		}
		return &nexus.HandlerError{
			Type:  nexus.HandlerErrorTypeUnauthenticated,
			Cause: errors.New(errMessage),
		}
	case codes.PermissionDenied:
		if !exposeDetails {
			errMessage = "permission denied"
		}
		return &nexus.HandlerError{
			Type:  nexus.HandlerErrorTypeUnauthorized,
			Cause: errors.New(errMessage),
		}
	case codes.NotFound:
		if !exposeDetails {
			errMessage = "not found"
		}
		return &nexus.HandlerError{
			Type:  nexus.HandlerErrorTypeNotFound,
			Cause: errors.New(errMessage),
		}
	case codes.ResourceExhausted:
		if !exposeDetails {
			errMessage = "resource exhausted"
		}
		return &nexus.HandlerError{
			Type:  nexus.HandlerErrorTypeResourceExhausted,
			Cause: errors.New(errMessage),
		}
	case codes.Unimplemented:
		if !exposeDetails {
			errMessage = "not implemented"
		}
		return &nexus.HandlerError{
			Type:  nexus.HandlerErrorTypeNotImplemented,
			Cause: errors.New(errMessage),
		}
	case codes.DeadlineExceeded:
		if !exposeDetails {
			errMessage = "request timeout"
		}
		return &nexus.HandlerError{
			Type:  nexus.HandlerErrorTypeUpstreamTimeout,
			Cause: errors.New(errMessage),
		}
	case codes.OK:
		return nil
	}
	if !exposeDetails {
		return &nexus.HandlerError{
			Type:  nexus.HandlerErrorTypeInternal,
			Cause: errors.New("internal error"),
		}
	}
	// Let the nexus SDK handle this for us (log and convert to an internal error).
	return err
}

func AdaptAuthorizeError(permissionDeniedError *serviceerror.PermissionDenied) error {
	if permissionDeniedError.Reason != "" {
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeUnauthorized, "permission denied: %s", permissionDeniedError.Reason)
	}
	return nexus.HandlerErrorf(nexus.HandlerErrorTypeUnauthorized, "permission denied")
}

func HandlerErrorTypeFromHTTPStatus(statusCode int) nexus.HandlerErrorType {
	switch statusCode {
	case http.StatusBadRequest:
		return nexus.HandlerErrorTypeBadRequest
	case http.StatusUnauthorized:
		return nexus.HandlerErrorTypeUnauthenticated
	case http.StatusForbidden:
		return nexus.HandlerErrorTypeUnauthorized
	case http.StatusNotFound:
		return nexus.HandlerErrorTypeNotFound
	case http.StatusTooManyRequests:
		return nexus.HandlerErrorTypeResourceExhausted
	case http.StatusInternalServerError:
		return nexus.HandlerErrorTypeInternal
	case http.StatusNotImplemented:
		return nexus.HandlerErrorTypeNotImplemented
	case http.StatusServiceUnavailable:
		return nexus.HandlerErrorTypeUnavailable
	case nexus.StatusUpstreamTimeout:
		return nexus.HandlerErrorTypeUpstreamTimeout
	default:
		return nexus.HandlerErrorTypeInternal
	}
}
