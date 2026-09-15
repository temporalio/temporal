package gocql

import (
	"context"
	"errors"
	"fmt"
	"testing"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"
)

// fakeRequestError implements gocql.RequestError for testing.
type fakeRequestError struct {
	code    int
	message string
}

func (e fakeRequestError) Code() int       { return e.code }
func (e fakeRequestError) Message() string { return e.message }
func (e fakeRequestError) Error() string   { return e.message }

func TestConvertError(t *testing.T) {
	const op = "TestOp"

	tests := map[string]struct {
		input     error
		checkFunc func(*testing.T, error)
	}{
		"nil": {
			input: nil,
			checkFunc: func(t *testing.T, err error) {
				assert.NoError(t, err)
			},
		},
		"context.DeadlineExceeded": {
			input: context.DeadlineExceeded,
			checkFunc: func(t *testing.T, err error) {
				var te *persistence.TimeoutError
				require.ErrorAs(t, err, &te)
				assert.Contains(t, te.Msg, op)
			},
		},
		"gocql.ErrTimeoutNoResponse": {
			input: gocql.ErrTimeoutNoResponse,
			checkFunc: func(t *testing.T, err error) {
				var te *persistence.TimeoutError
				require.ErrorAs(t, err, &te)
				assert.Contains(t, te.Msg, op)
			},
		},
		"gocql.ErrConnectionClosed": {
			input: gocql.ErrConnectionClosed,
			checkFunc: func(t *testing.T, err error) {
				var te *persistence.TimeoutError
				require.ErrorAs(t, err, &te)
				assert.Contains(t, te.Msg, op)
			},
		},
		"wrapped DeadlineExceeded": {
			input: fmt.Errorf("outer: %w", context.DeadlineExceeded),
			checkFunc: func(t *testing.T, err error) {
				var te *persistence.TimeoutError
				require.ErrorAs(t, err, &te)
			},
		},
		"gocql.ErrNotFound": {
			input: gocql.ErrNotFound,
			checkFunc: func(t *testing.T, err error) {
				var nf *serviceerror.NotFound
				require.ErrorAs(t, err, &nf)
				assert.Contains(t, nf.Message, op)
			},
		},
		"RequestErrWriteTimeout": {
			input: &gocql.RequestErrWriteTimeout{},
			checkFunc: func(t *testing.T, err error) {
				var te *persistence.TimeoutError
				require.ErrorAs(t, err, &te)
				assert.Contains(t, te.Msg, op)
			},
		},
		"RequestError ErrCodeOverloaded": {
			input: fakeRequestError{code: gocql.ErrCodeOverloaded, message: "overloaded"},
			checkFunc: func(t *testing.T, err error) {
				var re *serviceerror.ResourceExhausted
				require.ErrorAs(t, err, &re)
				assert.Equal(t, enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED, re.Cause)
				assert.Equal(t, enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM, re.Scope)
				assert.Contains(t, re.Message, op)
			},
		},
		"RequestError ErrCodeInvalid disk usage": {
			input: fakeRequestError{code: gocql.ErrCodeInvalid, message: "Disk usage exceeds failure threshold"},
			checkFunc: func(t *testing.T, err error) {
				var re *serviceerror.ResourceExhausted
				require.ErrorAs(t, err, &re)
				assert.Equal(t, enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_STORAGE_LIMIT, re.Cause)
				assert.Equal(t, enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM, re.Scope)
				assert.Contains(t, re.Message, op)
			},
		},
		"RequestError ErrCodeInvalid other": {
			input: fakeRequestError{code: gocql.ErrCodeInvalid, message: "some invalid query"},
			checkFunc: func(t *testing.T, err error) {
				var unavail *serviceerror.Unavailable
				require.ErrorAs(t, err, &unavail)
				assert.Contains(t, unavail.Message, op)
			},
		},
		"ResourceExhausted passthrough": {
			input: &serviceerror.ResourceExhausted{
				Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT,
				Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
				Message: "rps limit",
			},
			checkFunc: func(t *testing.T, err error) {
				var re *serviceerror.ResourceExhausted
				require.ErrorAs(t, err, &re)
				assert.Equal(t, enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT, re.Cause)
				assert.Equal(t, enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE, re.Scope)
				assert.Equal(t, "rps limit", re.Message)
			},
		},
		"unknown error becomes unavailable": {
			input: errors.New("some unknown error"),
			checkFunc: func(t *testing.T, err error) {
				var unavail *serviceerror.Unavailable
				require.ErrorAs(t, err, &unavail)
				assert.Contains(t, unavail.Message, op)
				assert.Contains(t, unavail.Message, "some unknown error")
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			tc.checkFunc(t, ConvertError(op, tc.input))
		})
	}
}
