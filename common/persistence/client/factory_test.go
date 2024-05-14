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

package client_test

import (
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/persistence/mock"
)

func TestFactoryImpl_NewHistoryTaskQueueManager(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		err  error
	}{
		{
			name: "No error",
			err:  nil,
		},
		{
			name: "Error",
			err:  errors.New("some error"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			dataStoreFactory := mock.NewMockDataStoreFactory(ctrl)
			dataStoreFactory.EXPECT().NewQueueV2().Return(nil, tc.err).AnyTimes()

			factory := client.NewFactory(
				dataStoreFactory,
				&config.Persistence{
					NumHistoryShards: 1,
				},
				nil,
				nil,
				nil,
				nil,
				"",
				nil,
				nil,
				nil,
			)
			historyTaskQueueManager, err := factory.NewHistoryTaskQueueManager()
			if tc.err != nil {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, historyTaskQueueManager)
		})
	}
}
