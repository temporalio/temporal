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

package history

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
)

func TestQueueFactoryLifetimeHooks(t *testing.T) {
	var (
		ctrl          = gomock.NewController(t)
		disabledQueue = NewMockQueueFactory(ctrl)
		enabledQueue  = NewMockQueueFactory(ctrl)
	)

	disabledQueue.EXPECT().Enabled().Return(false)
	enabledQueue.EXPECT().Enabled().Return(true)

	app := fx.New(
		fx.Provide(fx.Annotated{
			Group: QueueFactoryFxGroup,
			Target: func() QueueFactory {
				return disabledQueue
			},
		}, fx.Annotated{
			Group: QueueFactoryFxGroup,
			Target: func() QueueFactory {
				return enabledQueue
			},
		}),
		fx.Invoke(QueueFactoryLifetimeHooks),
	)
	require.NoError(t, app.Err())

	enabledQueue.EXPECT().Start()
	require.NoError(t, app.Start(context.Background()))

	enabledQueue.EXPECT().Stop()
	require.NoError(t, app.Stop(context.Background()))
}
