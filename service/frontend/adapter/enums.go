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
	"github.com/temporalio/temporal-proto/enums"
	"github.com/temporalio/temporal/.gen/go/shared"
)

func toThriftArchivalStatus(in enums.ArchivalStatus) *shared.ArchivalStatus {
	switch in {
	case enums.ArchivalStatusDefault:
		return nil
	case enums.ArchivalStatusDisabled:
		ret := shared.ArchivalStatusDisabled
		return &ret
	case enums.ArchivalStatusEnabled:
		ret := shared.ArchivalStatusEnabled
		return &ret
	}

	return nil
}

func toProtoArchivalStatus(in *shared.ArchivalStatus) enums.ArchivalStatus {
	if in == nil {
		return enums.ArchivalStatusDefault
	}

	switch *in {
	case shared.ArchivalStatusDisabled:
		return enums.ArchivalStatusDisabled
	case shared.ArchivalStatusEnabled:
		return enums.ArchivalStatusEnabled
	}

	return enums.ArchivalStatusDefault
}

func toProtoDomainStatus(in shared.DomainStatus) enums.DomainStatus {
	switch in {
	case shared.DomainStatusRegistered:
		return enums.DomainStatusRegistered
	case shared.DomainStatusDeprecated:
		return enums.DomainStatusDeprecated
	case shared.DomainStatusDeleted:
		return enums.DomainStatusDeleted
	}

	return enums.DomainStatusRegistered
}
