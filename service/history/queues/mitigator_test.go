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

package queues

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/tasks"
)

type (
	mitigatorSuite struct {
		suite.Suite
		*require.Assertions

		monitor   Monitor
		mitigator *mitigatorImpl
	}
)

func TestMitigatorSuite(t *testing.T) {
	s := new(mitigatorSuite)
	suite.Run(t, s)
}

func (s *mitigatorSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.monitor = newMonitor(tasks.CategoryTypeImmediate, nil)
	s.mitigator = newMitigator(
		s.monitor,
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
		dynamicconfig.GetIntPropertyFn(3),
	)
}

func (s *mitigatorSuite) TestReaderWatermarkAlert() {
	alert := Alert{
		AlertType: AlertTypeReaderStuck,
		AlertAttributesReaderStuck: &AlertAttributesReaderStuck{
			ReaderID:         1,
			CurrentWatermark: NewRandomKey(),
		},
	}

	action := s.mitigator.Mitigate(alert)
	s.IsType(&actionReaderStuck{}, action)

	s.Nil(
		s.mitigator.Mitigate(alert),
		"mitigator should have only one outstanding action for reader watermark alert",
	)

	action.(*actionReaderStuck).completionFn()

	action = s.mitigator.Mitigate(alert)
	s.NotNil(action)
	s.IsType(&actionReaderStuck{}, action)
}
