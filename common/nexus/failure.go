package nexus

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
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
	nf := nexus.Failure{
		Message:    failure.GetMessage(),
		StackTrace: failure.GetStackTrace(),
		Metadata:   failure.GetMetadata(),
		Details:    failure.GetDetails(),
	}
	if failure.GetCause() != nil {
		cause := ProtoFailureToNexusFailure(failure.GetCause())
		nf.Cause = &cause
	}
	return nf
}

// NexusFailureToProtoFailure converts a Nexus SDK Failure to a proto Nexus Failure.
// Always returns a non-nil value.
func NexusFailureToProtoFailure(failure nexus.Failure) *nexuspb.Failure {
	pf := &nexuspb.Failure{
		Message:    failure.Message,
		Metadata:   failure.Metadata,
		Details:    failure.Details,
		StackTrace: failure.StackTrace,
	}
	if failure.Cause != nil {
		pf.Cause = NexusFailureToProtoFailure(*failure.Cause)
	}
	return pf
}

type serializedHandlerError struct {
	Type              string `json:"type,omitempty"`
	RetryableOverride *bool  `json:"retryableOverride,omitempty"`
	// Bytes as base64 encoded string.
	EncodedAttributes string `json:"encodedAttributes,omitempty"`
}

// TemporalFailureToNexusFailure converts an API proto Failure to a Nexus SDK Failure setting the metadata "type" field to
// the proto fullname of the temporal API Failure message or the standard Nexus SDK failure types.
// Returns an error if the failure cannot be converted.
// Mutates the failure temporarily, unsetting the Message and StackTrace fields to avoid duplicating the information in
// the serialized failure. Mutating was chosen over cloning for performance reasons since this function may be called
// frequently.
func TemporalFailureToNexusFailure(failure *failurepb.Failure) (nexus.Failure, error) {
	var causep *nexus.Failure
	if failure.GetCause() != nil {
		var cause nexus.Failure
		var err error
		cause, err = TemporalFailureToNexusFailure(failure.GetCause())
		if err != nil {
			return nexus.Failure{}, err
		}
		causep = &cause
	}

	switch info := failure.GetFailureInfo().(type) {
	case *failurepb.Failure_NexusHandlerFailureInfo:
		var encodedAttributes string
		if failure.EncodedAttributes != nil {
			b, err := protojson.Marshal(failure.EncodedAttributes)
			if err != nil {
				return nexus.Failure{}, fmt.Errorf("failed to deserialize HandlerError attributes: %w", err)
			}
			encodedAttributes = base64.RawURLEncoding.EncodeToString(b)
		}
		var retryableOverride *bool
		// nolint:exhaustive,revive // There are only two valid values other than unspecified.
		switch info.NexusHandlerFailureInfo.GetRetryBehavior() {
		case enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE:
			val := true
			retryableOverride = &val
		case enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE:
			val := false
			retryableOverride = &val
		}

		handlerError := serializedHandlerError{
			Type:              info.NexusHandlerFailureInfo.GetType(),
			RetryableOverride: retryableOverride,
			EncodedAttributes: encodedAttributes,
		}

		details, err := json.Marshal(handlerError)
		if err != nil {
			return nexus.Failure{}, err
		}
		return nexus.Failure{
			Message:    failure.GetMessage(),
			StackTrace: failure.GetStackTrace(),
			Metadata: map[string]string{
				"type": "nexus.HandlerError",
			},
			Details: details,
			Cause:   causep,
		}, nil
	}
	// Unset message and stack trace so it's not serialized in the details.
	var message string
	message, failure.Message = failure.Message, ""
	var stackTrace string
	stackTrace, failure.StackTrace = failure.StackTrace, ""

	data, err := protojson.Marshal(failure)
	failure.Message = message
	failure.StackTrace = stackTrace
	if err != nil {
		return nexus.Failure{}, err
	}

	return nexus.Failure{
		Message:    failure.GetMessage(),
		StackTrace: failure.GetStackTrace(),
		Metadata: map[string]string{
			"type": failureTypeString,
		},
		Details: data,
		Cause:   causep,
	}, nil
}

// NexusFailureToTemporalFailure converts a Nexus Failure to an API proto Failure.
// If the failure metadata "type" field is set to the fullname of the temporal API Failure message, the failure is
// reconstructed using protojson.Unmarshal on the failure details field. Otherwise, the failure is reconstructed
// based on the known Nexus SDK failure types.
// Returns an error if the failure cannot be converted.
// nolint:revive // cognitive-complexity is high but justified to keep each case together
func NexusFailureToTemporalFailure(f nexus.Failure) (*failurepb.Failure, error) {
	apiFailure := &failurepb.Failure{
		Message:    f.Message,
		StackTrace: f.StackTrace,
	}

	if f.Metadata != nil {
		switch f.Metadata["type"] {
		case failureTypeString:
			opts := protojson.UnmarshalOptions{DiscardUnknown: true}
			if err := opts.Unmarshal(f.Details, apiFailure); err != nil {
				return nil, err
			}
			// Restore these fields as they are not included in the marshalled failure.
			apiFailure.Message = f.Message
			apiFailure.StackTrace = f.StackTrace
		case "nexus.OperationError":
			// Special case for OperationError that adapts from Nexus semantics to Temporal semantics.
			// Note that Temporal -> Temporal doesn't go through this code path, operation errors are always used as empty
			// wrappers for an underlying causes.
			var operationError *nexus.OperationError
			err := json.Unmarshal(f.Details, &operationError)
			if err != nil {
				return nil, fmt.Errorf("failed to deserialize OperationError: %w", err)
			}
			if operationError.State == nexus.OperationStateCanceled {
				// Canceled operation errors are represented as CanceledFailure in Temporal.
				apiFailure.FailureInfo = &failurepb.Failure_CanceledFailureInfo{
					CanceledFailureInfo: &failurepb.CanceledFailureInfo{},
				}
			} else {
				// Failed operation errors are represented as non-retryable ApplicationFailure in Temporal.
				apiFailure.FailureInfo = &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
						NonRetryable: true,
						Type:         "OperationError",
					},
				}
			}
		case "nexus.HandlerError":
			var se serializedHandlerError
			err := json.Unmarshal(f.Details, &se)
			if err != nil {
				return nil, fmt.Errorf("failed to deserialize HandlerError: %w", err)
			}
			var retryBehavior enumspb.NexusHandlerErrorRetryBehavior
			if se.RetryableOverride == nil {
				retryBehavior = enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_UNSPECIFIED
			} else if *se.RetryableOverride {
				retryBehavior = enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE
			} else {
				retryBehavior = enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE
			}
			apiFailure.FailureInfo = &failurepb.Failure_NexusHandlerFailureInfo{
				NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{
					Type:          se.Type,
					RetryBehavior: retryBehavior,
				},
			}
			if len(se.EncodedAttributes) > 0 {
				decoded, err := base64.RawURLEncoding.DecodeString(se.EncodedAttributes)
				if err != nil {
					return nil, fmt.Errorf("failed to decode base64 HandlerError attributes: %w", err)
				}
				apiFailure.EncodedAttributes = &commonpb.Payload{}
				if err := protojson.Unmarshal(decoded, apiFailure.EncodedAttributes); err != nil {
					return nil, fmt.Errorf("failed to deserialize HandlerError attributes: %w", err)
				}
			}
		default:
			// We don't recognize this type, convert to a generic ApplicationFailure and preserve the original Nexus failure
			// as serialized details.
			applicationFailureInfo, err := nexusFailureMetadataToApplicationFailureInfo(f)
			if err != nil {
				return nil, fmt.Errorf("failed to serialize Nexus failure: %w", err)
			}
			apiFailure.FailureInfo = applicationFailureInfo
		}
	} else if len(f.Details) > 0 {
		// We don't recognize this type, convert to a generic ApplicationFailure and preserve the original Nexus failure as
		// serialized details.
		applicationFailureInfo, err := nexusFailureMetadataToApplicationFailureInfo(f)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize Nexus failure: %w", err)
		}
		apiFailure.FailureInfo = applicationFailureInfo
	}

	if f.Cause != nil {
		var err error
		apiFailure.Cause, err = NexusFailureToTemporalFailure(*f.Cause)
		if err != nil {
			return nil, err
		}
	}
	return apiFailure, nil
}

func nexusFailureMetadataToApplicationFailureInfo(failure nexus.Failure) (*failurepb.Failure_ApplicationFailureInfo, error) {
	var payloads *commonpb.Payloads
	if len(failure.Metadata) > 0 || len(failure.Details) > 0 {
		// Delete before serializing (note the failure here is passed by value).
		failure.Message = ""
		failure.StackTrace = ""
		data, err := json.Marshal(failure)
		if err != nil {
			return nil, err
		}
		payloads = &commonpb.Payloads{
			Payloads: []*commonpb.Payload{
				{
					Metadata: map[string][]byte{
						"encoding": []byte("json/plain"),
					},
					Data: data,
				},
			},
		}
	}
	return &failurepb.Failure_ApplicationFailureInfo{
		ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
			Details: payloads,
		},
	}, nil
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
			Type:    nexus.HandlerErrorTypeBadRequest,
			Message: errMessage,
		}
	case codes.Aborted, codes.Unavailable:
		if !exposeDetails {
			errMessage = "service unavailable"
		}
		return &nexus.HandlerError{
			Type:    nexus.HandlerErrorTypeUnavailable,
			Message: errMessage,
		}
	case codes.Canceled:
		// TODO: This should have a different status code (e.g. 499 which is semi standard but not supported by nexus).
		// The important thing is that the request is retryable, internal serves that purpose.
		if !exposeDetails {
			errMessage = "canceled"
		}
		return &nexus.HandlerError{
			Type:    nexus.HandlerErrorTypeInternal,
			Message: errMessage,
		}
	case codes.DataLoss, codes.Internal, codes.Unknown:
		if !exposeDetails {
			errMessage = "internal error"
		}
		return &nexus.HandlerError{
			Type:    nexus.HandlerErrorTypeInternal,
			Message: errMessage,
		}
	case codes.Unauthenticated:
		if !exposeDetails {
			errMessage = "authentication failed"
		}
		return &nexus.HandlerError{
			Type:    nexus.HandlerErrorTypeUnauthenticated,
			Message: errMessage,
		}
	case codes.PermissionDenied:
		if !exposeDetails {
			errMessage = "permission denied"
		}
		return &nexus.HandlerError{
			Type:    nexus.HandlerErrorTypeUnauthorized,
			Message: errMessage,
		}
	case codes.NotFound:
		if !exposeDetails {
			errMessage = "not found"
		}
		return &nexus.HandlerError{
			Type:    nexus.HandlerErrorTypeNotFound,
			Message: errMessage,
		}
	case codes.ResourceExhausted:
		if !exposeDetails {
			errMessage = "resource exhausted"
		}
		return &nexus.HandlerError{
			Type:    nexus.HandlerErrorTypeResourceExhausted,
			Message: errMessage,
		}
	case codes.Unimplemented:
		if !exposeDetails {
			errMessage = "not implemented"
		}
		return &nexus.HandlerError{
			Type:    nexus.HandlerErrorTypeNotImplemented,
			Message: errMessage,
		}
	case codes.DeadlineExceeded:
		if !exposeDetails {
			errMessage = "request timeout"
		}
		return &nexus.HandlerError{
			Type:    nexus.HandlerErrorTypeUpstreamTimeout,
			Message: errMessage,
		}
	case codes.OK:
		return nil
	}
	if !exposeDetails {
		return &nexus.HandlerError{
			Type:    nexus.HandlerErrorTypeInternal,
			Message: "internal error",
		}
	}
	// Let the nexus SDK handle this for us (log and convert to an internal error).
	return err
}

func AdaptAuthorizeError(permissionDeniedError *serviceerror.PermissionDenied) error {
	if permissionDeniedError.Reason != "" {
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeUnauthorized, "permission denied: %s", permissionDeniedError.Reason)
	}
	return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeUnauthorized, "permission denied")
}
