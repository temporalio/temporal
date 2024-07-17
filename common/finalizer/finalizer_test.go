// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package finalizer_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.temporal.io/server/common/clock"
	. "go.temporal.io/server/common/finalizer"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/internal/goro"
)

func TestFinalizer(t *testing.T) {

	t.Run("register", func(t *testing.T) {
		t.Run("succeeds", func(t *testing.T) {
			f := newFinalizer()
			require.NoError(t, f.Register("1", nil))
			require.NoError(t, f.Deregister("1"))
		})

		t.Run("fails when ID already registered", func(t *testing.T) {
			f := newFinalizer()
			require.NoError(t, f.Register("1", nil))
			require.ErrorIs(t, f.Register("1", nil), FinalizerDuplicateIdErr)
		})

		t.Run("fails after already run before", func(t *testing.T) {
			f := newFinalizer()
			f.Run(newPool(), 1*time.Second)
			require.ErrorIs(t, f.Register("1", nil), FinalizerAlreadyDoneErr)
		})
	})

	t.Run("deregister", func(t *testing.T) {
		t.Run("succeeds", func(t *testing.T) {
			f := newFinalizer()
			require.NoError(t, f.Register("1", nil))
			require.NoError(t, f.Deregister("1"))
			require.Zero(t, f.Run(newPool(), 1*time.Second))
		})

		t.Run("fails if callback does not exist", func(t *testing.T) {
			f := newFinalizer()
			require.ErrorIs(t, f.Deregister("does-not-exist"), FinalizerUnknownIdErr)
		})

		t.Run("fails after already run before", func(t *testing.T) {
			f := newFinalizer()
			f.Run(newPool(), 1*time.Second)
			require.ErrorIs(t, f.Deregister("1"), FinalizerAlreadyDoneErr)
		})
	})

	t.Run("run", func(t *testing.T) {
		t.Run("invokes all callbacks", func(t *testing.T) {
			f := newFinalizer()

			var completed atomic.Int32
			for i := 0; i < 5; i += 1 {
				require.NoError(t, f.Register(
					fmt.Sprintf("%v", i),
					func(ctx context.Context) error {
						completed.Add(1)
						//nolint:forbidigo
						time.Sleep(50 * time.Millisecond)
						return nil
					}))
			}

			require.EqualValues(t, 5, f.Run(newPool(), 1*time.Second))
			require.EqualValues(t, 5, completed.Load())
		})

		t.Run("returns once timeout has been reached", func(t *testing.T) {
			f := newFinalizer()
			timeout := 50 * time.Millisecond

			require.NoError(t, f.Register(
				"before-timeout",
				func(ctx context.Context) error {
					return nil
				}))

			require.NoError(t, f.Register(
				"after-timeout",
				func(ctx context.Context) error {
					//nolint:forbidigo
					time.Sleep(2 * timeout)
					return nil
				}))

			completed := f.Run(newPool(), timeout)
			require.EqualValues(t, 1, completed, "expected only one callback to complete")
		})

		t.Run("does not execute more than once", func(t *testing.T) {
			f := newFinalizer()
			require.NoError(t, f.Register(
				"0",
				func(ctx context.Context) error {
					return nil
				}))
			require.EqualValues(t, 1, f.Run(newPool(), 1*time.Second))                            // 1st call
			require.Zero(t, f.Run(newPool(), 1*time.Second), "expected no callbacks to complete") // 2nd call
		})
	})
}

func newFinalizer() *Finalizer {
	return New(log.NewNoopLogger())
}

func newPool() *goro.AdaptivePool {
	return goro.NewAdaptivePool(clock.NewRealTimeSource(), 1, 2, 10*time.Millisecond, 10)
}
