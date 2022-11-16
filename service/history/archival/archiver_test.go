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

package archival

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"

	carchiver "go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/testing/mocksdk"
)

func TestArchiver(t *testing.T) {
	for _, c := range []struct {
		Name                 string
		ArchiveHistoryErr    error
		ArchiveVisibilityErr error
		RateLimiterWaitErr   error
		Targets              []Target

		ExpectArchiveHistory    bool
		ExpectArchiveVisibility bool
		ExpectedMetrics         []expectedMetric
		ExpectedReturnErrors    []string
	}{
		{
			Name:    "No targets",
			Targets: []Target{},

			ExpectedMetrics: []expectedMetric{
				{
					Metric: metrics.ArchiverArchiveLatency.GetMetricName(),
					Tags:   []metrics.Tag{metrics.StringTag("status", "ok")},
				},
			},
		},
		{
			Name:    "Invalid target",
			Targets: []Target{Target("oops")},

			ExpectedMetrics: []expectedMetric{
				{
					Metric: metrics.ArchiverArchiveLatency.GetMetricName(),
					Tags:   []metrics.Tag{metrics.StringTag("status", "err")},
				},
			},
			ExpectedReturnErrors: []string{
				"unknown archival target: oops",
			},
		},
		{
			Name:              "History archival succeeds",
			Targets:           []Target{TargetHistory},
			ArchiveHistoryErr: nil,

			ExpectedMetrics: []expectedMetric{
				{
					Metric: metrics.ArchiverArchiveLatency.GetMetricName(),
					Tags:   []metrics.Tag{metrics.StringTag("status", "ok")},
				},
				{
					Metric: metrics.ArchiverArchiveTargetLatency.GetMetricName(),
					Tags: []metrics.Tag{
						metrics.StringTag("target", "history"),
						metrics.StringTag("status", "ok"),
					},
				},
			},
			ExpectArchiveHistory: true,
		},
		{
			Name:              "History archival fails",
			Targets:           []Target{TargetHistory},
			ArchiveHistoryErr: errors.New("example archive history error"),

			ExpectArchiveHistory: true,
			ExpectedMetrics: []expectedMetric{
				{
					Metric: metrics.ArchiverArchiveLatency.GetMetricName(),
					Tags:   []metrics.Tag{metrics.StringTag("status", "err")},
				},
				{
					Metric: metrics.ArchiverArchiveTargetLatency.GetMetricName(),
					Tags: []metrics.Tag{
						metrics.StringTag("target", "history"),
						metrics.StringTag("status", "err"),
					},
				},
			},
			ExpectedReturnErrors: []string{"example archive history err"},
		},
		{
			Name:    "Visibility archival succeeds",
			Targets: []Target{TargetVisibility},

			ExpectedMetrics: []expectedMetric{
				{
					Metric: metrics.ArchiverArchiveLatency.GetMetricName(),
					Tags:   []metrics.Tag{metrics.StringTag("status", "ok")},
				},
				{
					Metric: metrics.ArchiverArchiveTargetLatency.GetMetricName(),
					Tags: []metrics.Tag{
						metrics.StringTag("target", "visibility"),
						metrics.StringTag("status", "ok"),
					},
				},
			},
			ExpectArchiveVisibility: true,
		},
		{
			Name:                 "History succeeds but visibility fails",
			Targets:              []Target{TargetHistory, TargetVisibility},
			ArchiveVisibilityErr: errors.New("example archive visibility error"),

			ExpectArchiveHistory:    true,
			ExpectArchiveVisibility: true,
			ExpectedReturnErrors:    []string{"example archive visibility error"},
			ExpectedMetrics: []expectedMetric{
				{
					Metric: metrics.ArchiverArchiveLatency.GetMetricName(),
					Tags:   []metrics.Tag{metrics.StringTag("status", "err")},
				},
				{
					Metric: metrics.ArchiverArchiveTargetLatency.GetMetricName(),
					Tags: []metrics.Tag{
						metrics.StringTag("target", "history"),
						metrics.StringTag("status", "ok"),
					},
				},
				{
					Metric: metrics.ArchiverArchiveTargetLatency.GetMetricName(),
					Tags: []metrics.Tag{
						metrics.StringTag("target", "visibility"),
						metrics.StringTag("status", "err"),
					},
				},
			},
		},
		{
			Name:              "Visibility succeeds but history fails",
			Targets:           []Target{TargetHistory, TargetVisibility},
			ArchiveHistoryErr: errors.New("example archive history error"),

			ExpectArchiveHistory:    true,
			ExpectArchiveVisibility: true,
			ExpectedReturnErrors:    []string{"example archive history error"},
			ExpectedMetrics: []expectedMetric{
				{
					Metric: metrics.ArchiverArchiveLatency.GetMetricName(),
					Tags:   []metrics.Tag{metrics.StringTag("status", "err")},
				},
				{
					Metric: metrics.ArchiverArchiveTargetLatency.GetMetricName(),
					Tags: []metrics.Tag{
						metrics.StringTag("target", "history"),
						metrics.StringTag("status", "err"),
					},
				},
				{
					Metric: metrics.ArchiverArchiveTargetLatency.GetMetricName(),
					Tags: []metrics.Tag{
						metrics.StringTag("target", "visibility"),
						metrics.StringTag("status", "ok"),
					},
				},
			},
		},
		{
			Name:    "Both targets succeed",
			Targets: []Target{TargetHistory, TargetVisibility},

			ExpectArchiveHistory:    true,
			ExpectArchiveVisibility: true,
			ExpectedMetrics: []expectedMetric{
				{
					Metric: metrics.ArchiverArchiveLatency.GetMetricName(),
					Tags:   []metrics.Tag{metrics.StringTag("status", "ok")},
				},
				{
					Metric: metrics.ArchiverArchiveTargetLatency.GetMetricName(),
					Tags: []metrics.Tag{
						metrics.StringTag("target", "history"),
						metrics.StringTag("status", "ok"),
					},
				},
				{
					Metric: metrics.ArchiverArchiveTargetLatency.GetMetricName(),
					Tags: []metrics.Tag{
						metrics.StringTag("target", "visibility"),
						metrics.StringTag("status", "ok"),
					},
				},
			},
		},
		{
			Name:               "Rate limit hit",
			Targets:            []Target{TargetHistory, TargetVisibility},
			RateLimiterWaitErr: errors.New("didn't acquire rate limiter tokens in time"),

			ExpectedReturnErrors: []string{
				"archival rate limited: didn't acquire rate limiter tokens in time",
			},
			ExpectedMetrics: []expectedMetric{
				{
					Metric: metrics.ArchiverArchiveLatency.GetMetricName(),
					Tags:   []metrics.Tag{metrics.StringTag("status", "rate_limit_exceeded")},
				},
			},
		},
	} {
		t.Run(c.Name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			controller := gomock.NewController(t)
			archiverProvider := provider.NewMockArchiverProvider(controller)
			historyArchiver := carchiver.NewMockHistoryArchiver(controller)
			visibilityArchiver := carchiver.NewMockVisibilityArchiver(controller)
			metricsHandler := metrics.NewMockMetricsHandler(controller)
			metricsHandler.EXPECT().WithTags(metrics.OperationTag(metrics.ArchiverClientScope)).Return(metricsHandler)
			sdkClient := mocksdk.NewMockClient(controller)
			sdkClientFactory := sdk.NewMockClientFactory(controller)
			sdkClientFactory.EXPECT().GetSystemClient().Return(sdkClient).AnyTimes()
			logRecorder := &errorLogRecorder{
				T:      t,
				Logger: log.NewNoopLogger(),
			}
			for _, m := range c.ExpectedMetrics {
				metricsHandler.EXPECT().Timer(m.Metric).Return(metrics.NoopTimerMetricFunc)
			}
			historyURI, err := carchiver.NewURI("test:///history/archival")
			require.NoError(t, err)
			if c.ExpectArchiveHistory {
				archiverProvider.EXPECT().GetHistoryArchiver(gomock.Any(), gomock.Any()).Return(historyArchiver, nil)
				historyArchiver.EXPECT().Archive(gomock.Any(), historyURI, gomock.Any()).Return(c.ArchiveHistoryErr)
			}
			visibilityURI, err := carchiver.NewURI("test:///visibility/archival")
			require.NoError(t, err)
			if c.ExpectArchiveVisibility {
				archiverProvider.EXPECT().GetVisibilityArchiver(gomock.Any(), gomock.Any()).
					Return(visibilityArchiver, nil)
				visibilityArchiver.EXPECT().Archive(gomock.Any(), visibilityURI, gomock.Any()).
					Return(c.ArchiveVisibilityErr)
			}
			rateLimiter := quotas.NewMockRateLimiter(controller)
			rateLimiter.EXPECT().WaitN(gomock.Any(), 2).Return(c.RateLimiterWaitErr)

			archiver := NewArchiver(archiverProvider, logRecorder, metricsHandler, rateLimiter)
			_, err = archiver.Archive(ctx, &Request{
				HistoryURI:    historyURI.String(),
				VisibilityURI: visibilityURI.String(),
				Targets:       c.Targets,
			})

			if len(c.ExpectedReturnErrors) > 0 {
				require.Error(t, err)
				assert.Len(t, multierr.Errors(err), len(c.ExpectedReturnErrors))
				for _, e := range c.ExpectedReturnErrors {
					assert.Contains(t, err.Error(), e)
				}
			} else {
				assert.NoError(t, err)
			}

			var expectedLogErrs []string
			if c.ArchiveHistoryErr != nil {
				expectedLogErrs = append(expectedLogErrs, c.ArchiveHistoryErr.Error())
			}
			if c.ArchiveVisibilityErr != nil {
				expectedLogErrs = append(expectedLogErrs, c.ArchiveVisibilityErr.Error())
			}
			assert.ElementsMatch(t, expectedLogErrs, logRecorder.ErrorMessages)
		})
	}
}

type expectedMetric struct {
	Metric string
	Tags   []metrics.Tag
}

type errorLogRecorder struct {
	log.Logger
	T             testing.TB
	ErrorMessages []string
}

func (r *errorLogRecorder) Error(msg string, tags ...tag.Tag) {
	for _, t := range tags {
		if t.Key() == "error" {
			value := t.Value()
			message, ok := value.(fmt.Stringer)
			require.True(r.T, ok)
			r.ErrorMessages = append(r.ErrorMessages, message.String())
		}
	}
}
