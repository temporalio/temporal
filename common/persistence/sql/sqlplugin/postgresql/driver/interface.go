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

package driver

import (
	"github.com/jmoiron/sqlx"
)

const (
	// check http://www.postgresql.org/docs/9.3/static/errcodes-appendix.html
	dupEntryCode            = "23505"
	dupDatabaseCode         = "42P04"
	readOnlyTransactionCode = "25006"
	cannotConnectNowCode    = "57P03"
	featureNotSupportedCode = "0A000"

	// Unsupported "feature" messages to look for
	cannotSetReadWriteModeDuringRecoveryMsg = "cannot set transaction read-write mode during recovery"
)

type Driver interface {
	CreateConnection(dsn string) (*sqlx.DB, error)
	IsDupEntryError(error) bool
	IsDupDatabaseError(error) bool
	IsConnNeedsRefreshError(error) bool
}

func isConnNeedsRefreshError(code, message string) bool {
	if code == readOnlyTransactionCode || code == cannotConnectNowCode {
		return true
	}
	if code == featureNotSupportedCode && message == cannotSetReadWriteModeDuringRecoveryMsg {
		return true
	}
	return false
}
