// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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
