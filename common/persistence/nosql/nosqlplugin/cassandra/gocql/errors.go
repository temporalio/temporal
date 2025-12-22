package gocql

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/gocql/gocql"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"
)

func ConvertError(
	operation string,
	err error,
) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, gocql.ErrTimeoutNoResponse) || errors.Is(err, gocql.ErrConnectionClosed) {
		return &persistence.TimeoutError{Msg: fmt.Sprintf("operation %v encountered %v", operation, err.Error())}
	}
	if errors.Is(err, gocql.ErrNotFound) {
		return serviceerror.NewNotFoundf("operation %v encountered %v", operation, err.Error())
	}

	var cqlTimeoutErr *gocql.RequestErrWriteTimeout
	if errors.As(err, &cqlTimeoutErr) {
		return &persistence.TimeoutError{Msg: fmt.Sprintf("operation %v encountered %v", operation, cqlTimeoutErr.Error())}
	}

	var cqlRequestErr gocql.RequestError
	if errors.As(err, &cqlRequestErr) {
		if cqlRequestErr.Code() == gocql.ErrCodeOverloaded {
			return &serviceerror.ResourceExhausted{
				Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED,
				Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM,
				Message: fmt.Sprintf("operation %v encountered %v", operation, cqlRequestErr.Error()),
			}
		}

		if cqlRequestErr.Code() == gocql.ErrCodeInvalid {
			// NB: See https://cassandra.apache.org/_/blog/Apache-Cassandra-4.1-Features-Guardrails-Framework.html
			if strings.Contains(strings.ToLower(cqlRequestErr.Message()), "disk usage exceeds failure threshold") {
				return &serviceerror.ResourceExhausted{
					Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_STORAGE_LIMIT,
					Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM,
					Message: fmt.Sprintf("operation %v encountered %v", operation, cqlRequestErr.Error()),
				}
			}

			return serviceerror.NewUnavailablef("operation %v encountered %v", operation, cqlRequestErr.Error())
		}
	}

	return serviceerror.NewUnavailablef("operation %v encountered %v", operation, err.Error())
}

func IsNotFoundError(err error) bool {
	return errors.Is(err, gocql.ErrNotFound)
}
