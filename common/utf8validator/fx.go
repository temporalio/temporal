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

package utf8validator

import (
	"go.uber.org/fx"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

var Module = fx.Options(
	fx.Provide(utf8ValidatorProvider),
)

func utf8ValidatorProvider(
	logger log.Logger,
	metrics metrics.Handler,
	col *dynamicconfig.Collection,
) *Validator {
	return newValidator(
		logger,
		metrics,
		col.GetFloat64Property(dynamicconfig.ValidateUTF8SampleRPCRequest, 0.0),
		col.GetFloat64Property(dynamicconfig.ValidateUTF8SampleRPCResponse, 0.0),
		col.GetFloat64Property(dynamicconfig.ValidateUTF8SamplePersistence, 0.0),
		col.GetBoolProperty(dynamicconfig.ValidateUTF8FailRPCRequest, false),
		col.GetBoolProperty(dynamicconfig.ValidateUTF8FailRPCResponse, false),
		col.GetBoolProperty(dynamicconfig.ValidateUTF8FailPersistence, false),
	)
}
