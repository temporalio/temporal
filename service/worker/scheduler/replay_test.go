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

package scheduler_test

import (
	"compress/gzip"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/service/worker/scheduler"
)

// TestReplays tests workflow logic backwards compatibility from previous versions.
// Whenever there's a change in logic, consider capturing a new history with the
// testdata/generate_history.sh script and checking it in.
func TestReplays(t *testing.T) {
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(scheduler.SchedulerWorkflow, workflow.RegisterOptions{Name: scheduler.WorkflowType})

	files, err := filepath.Glob("testdata/replay_*.json.gz")
	require.NoError(t, err)

	logger := log.NewSdkLogger(log.NewTestLogger())

	for _, filename := range files {
		logger.Info("Replaying", "file", filename)
		f, err := os.Open(filename)
		require.NoError(t, err)
		r, err := gzip.NewReader(f)
		require.NoError(t, err)
		history, err := client.HistoryFromJSON(r, client.HistoryJSONOptions{})
		require.NoError(t, err)
		err = replayer.ReplayWorkflowHistory(logger, history)
		require.NoError(t, err)
		_ = r.Close()
		_ = f.Close()
	}
}
