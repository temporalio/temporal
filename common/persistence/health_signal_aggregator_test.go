package persistence

import (
	"testing"

	"go.temporal.io/api/serviceerror"
)

func Test_isUnhealthyError(t *testing.T) {
	type args struct {
		err error
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "nil error",
			args: args{err: nil},
			want: false,
		},
		{
			name: "context canceled error",
			args: args{err: &serviceerror.Canceled{
				Message: "context canceled",
			}},
			want: true,
		},
		{
			name: "context deadline exceeded error",
			args: args{err: &serviceerror.DeadlineExceeded{
				Message: "context deadline exceeded",
			}},
			want: true,
		},
		{
			name: "AppendHistoryTimeoutError",
			args: args{err: &AppendHistoryTimeoutError{
				Msg: "append history timeout",
			}},
			want: true,
		},
		{
			name: "InvalidPersistenceRequestError",
			args: args{err: &InvalidPersistenceRequestError{
				Msg: "invalid persistence request",
			}},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isUnhealthyError(tt.args.err); got != tt.want {
				t.Errorf("isUnhealthyError() = %v, want %v", got, tt.want)
			}
		})
	}
}
