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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/multierr"

	"go.temporal.io/api/common/v1"
	carchiver "go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/testing/mocksdk"
	"go.temporal.io/server/service/history/configs"
)

func TestArchiver(t *testing.T) {
	for _, c := range []struct {
		Name                 string
		ArchiveHistoryErr    error
		ArchiveVisibilityErr error
		RateLimiterWaitErr   error
		Targets              []Target
		SearchAttributes     *common.SearchAttributes
		SearchAttributesErr  error
		NameTypeMap          searchattribute.NameTypeMap
		NameTypeMapErr       error
		NilHistoryUri        bool

		ExpectArchiveHistory    bool
		ExpectArchiveVisibility bool
		ExpectedReturnErrors    []string
	}{
		{
			Name:    "No targets",
			Targets: []Target{},
		},
		{
			Name:    "Invalid target",
			Targets: []Target{Target("oops")},

			ExpectedReturnErrors: []string{
				"unknown archival target: oops",
			},
		},
		{
			Name:              "History archival succeeds",
			Targets:           []Target{TargetHistory},
			ArchiveHistoryErr: nil,

			ExpectArchiveHistory: true,
		},
		{
			Name:              "History archival fails",
			Targets:           []Target{TargetHistory},
			ArchiveHistoryErr: errors.New("example archive history error"),

			ExpectArchiveHistory: true,
			ExpectedReturnErrors: []string{"example archive history err"},
		},
		{
			Name:    "Visibility archival succeeds",
			Targets: []Target{TargetVisibility},

			ExpectArchiveVisibility: true,
		},
		{
			Name:          "Visibility archival succeeds with nil HistoryURI",
			Targets:       []Target{TargetVisibility},
			NilHistoryUri: true,

			ExpectArchiveVisibility: true,
		},
		{
			Name:                 "History succeeds but visibility fails",
			Targets:              []Target{TargetHistory, TargetVisibility},
			ArchiveVisibilityErr: errors.New("example archive visibility error"),

			ExpectArchiveHistory:    true,
			ExpectArchiveVisibility: true,
			ExpectedReturnErrors:    []string{"example archive visibility error"},
		},
		{
			Name:              "Visibility succeeds but history fails",
			Targets:           []Target{TargetHistory, TargetVisibility},
			ArchiveHistoryErr: errors.New("example archive history error"),

			ExpectArchiveHistory:    true,
			ExpectArchiveVisibility: true,
			ExpectedReturnErrors:    []string{"example archive history error"},
		},
		{
			Name:    "Both targets succeed",
			Targets: []Target{TargetHistory, TargetVisibility},

			ExpectArchiveHistory:    true,
			ExpectArchiveVisibility: true,
		},
		{
			Name:               "Rate limit hit",
			Targets:            []Target{TargetHistory, TargetVisibility},
			RateLimiterWaitErr: errors.New("didn't acquire rate limiter tokens in time"),

			ExpectedReturnErrors: []string{
				"archival rate limited: didn't acquire rate limiter tokens in time",
			},
		},
		{
			Name:    "Search attribute with no embedded type information",
			Targets: []Target{TargetVisibility},
			SearchAttributes: &common.SearchAttributes{IndexedFields: map[string]*common.Payload{
				"Text01": payload.EncodeString("value"),
			}},
			NameTypeMap: searchattribute.TestNameTypeMap,

			ExpectArchiveVisibility: true,
		},
		{
			Name:    "Search attribute missing in type map",
			Targets: []Target{TargetVisibility},
			SearchAttributes: &common.SearchAttributes{IndexedFields: map[string]*common.Payload{
				"Text01": payload.EncodeString("value"),
			}},
			NameTypeMap: searchattribute.NameTypeMap{},

			ExpectedReturnErrors: []string{
				"invalid search attribute type: Unspecified",
			},
		},
		{
			Name:    "Error getting name type map from search attribute provider",
			Targets: []Target{TargetVisibility},
			SearchAttributes: &common.SearchAttributes{IndexedFields: map[string]*common.Payload{
				"Text01": payload.EncodeString("value"),
			}},
			NameTypeMapErr: errors.New("name-type-map-err"),

			ExpectedReturnErrors: []string{
				"name-type-map-err",
			},
		},
	} {
		c := c // capture range variable
		t.Run(c.Name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			controller := gomock.NewController(t)
			archiverProvider := provider.NewMockArchiverProvider(controller)
			historyArchiver := carchiver.NewMockHistoryArchiver(controller)
			visibilityArchiver := carchiver.NewMockVisibilityArchiver(controller)
			metricsHandler := metrics.NoopMetricsHandler
			sdkClient := mocksdk.NewMockClient(controller)
			sdkClientFactory := sdk.NewMockClientFactory(controller)
			sdkClientFactory.EXPECT().GetSystemClient().Return(sdkClient).AnyTimes()

			var historyURI carchiver.URI
			var err error
			if !c.NilHistoryUri {
				historyURI, err = carchiver.NewURI("test:///history/archival")
			}
			require.NoError(t, err)

			if c.ExpectArchiveHistory {
				archiverProvider.EXPECT().GetHistoryArchiver(gomock.Any(), gomock.Any()).Return(historyArchiver, nil)
				historyArchiver.EXPECT().Archive(gomock.Any(), historyURI, gomock.Any()).Return(c.ArchiveHistoryErr)
			}

			visibilityURI, err := carchiver.NewURI("test:///visibility/archival")
			require.NoError(t, err)
			archiverProvider.EXPECT().GetVisibilityArchiver(gomock.Any(), gomock.Any()).
				Return(visibilityArchiver, nil).AnyTimes()

			if c.ExpectArchiveVisibility {
				visibilityArchiver.EXPECT().Archive(gomock.Any(), visibilityURI, gomock.Any()).
					Return(c.ArchiveVisibilityErr)
			}

			rateLimiter := quotas.NewMockRateLimiter(controller)
			rateLimiter.EXPECT().WaitN(gomock.Any(), len(c.Targets)).Return(c.RateLimiterWaitErr)

			searchAttributeProvider := searchattribute.NewMockProvider(controller)
			searchAttributeProvider.EXPECT().GetSearchAttributes(gomock.Any(), gomock.Any()).Return(
				c.NameTypeMap, c.NameTypeMapErr,
			).AnyTimes()

			visibilityManager := manager.NewMockVisibilityManager(controller)
			visibilityManager.EXPECT().GetIndexName().Return("index-name").AnyTimes()

			// we need this channel to get the Archiver which is created asynchronously
			archivers := make(chan Archiver, 1)
			// we make an app here so that we can test that the Module is working as intended
			app := fx.New(
				fx.Supply(fx.Annotate(archiverProvider, fx.As(new(provider.ArchiverProvider)))),
				fx.Supply(fx.Annotate(log.NewNoopLogger(), fx.As(new(log.Logger)))),
				fx.Supply(fx.Annotate(metricsHandler, fx.As(new(metrics.Handler)))),
				fx.Supply(fx.Annotate(searchAttributeProvider, fx.As(new(searchattribute.Provider)))),
				fx.Supply(fx.Annotate(visibilityManager, fx.As(new(manager.VisibilityManager)))),
				fx.Supply(&configs.Config{
					ArchivalBackendMaxRPS: func() float64 {
						return 42.0
					},
				}),
				Module,
				fx.Decorate(func(rl quotas.RateLimiter) quotas.RateLimiter {
					// we need to decorate the rate limiter so that we can use the mock
					// we also verify that the rate being used is equal to the one in the config
					assert.Equal(t, 42.0, rl.Rate())
					return rateLimiter
				}),
				fx.Invoke(func(a Archiver) {
					// after all parameters are provided, we get the Archiver and put it in the channel
					// so that we can use it in the test
					archivers <- a
				}),
			)
			require.NoError(t, app.Err())
			// we need to start the app for fx.Invoke to be called, so that we can get the Archiver
			require.NoError(t, app.Start(ctx))

			defer func() {
				require.NoError(t, app.Stop(ctx))
			}()

			archiver := <-archivers
			searchAttributes := c.SearchAttributes
			_, err = archiver.Archive(ctx, &Request{
				HistoryURI:       historyURI,
				VisibilityURI:    visibilityURI,
				Targets:          c.Targets,
				SearchAttributes: searchAttributes,
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
		})
	}
}
