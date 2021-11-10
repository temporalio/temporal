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
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package metrics

import (
	"github.com/uber-go/tally/v4"

	"go.temporal.io/server/common/log"
)

// TallyReporter is a base class for reporting metrics to Tally.
type TallyReporter struct {
	scope        tally.Scope
	clientConfig *ClientConfig
}

func newTallyReporter(
	scope tally.Scope,
	clientConfig *ClientConfig,
) *TallyReporter {
	return &TallyReporter{
		scope:        scope,
		clientConfig: clientConfig,
	}
}

func (tr *TallyReporter) NewClient(logger log.Logger, serviceIdx ServiceIdx) (Client, error) {
	return NewClient(tr.clientConfig, tr.scope, serviceIdx), nil
}

func (tr *TallyReporter) GetScope() tally.Scope {
	return tr.scope
}

func (tr *TallyReporter) Stop(logger log.Logger) {
	// noop
}
