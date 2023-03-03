// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021 Datadog, Inc.

package temporaltest

import (
	"testing"
)

type testLogger struct {
	t *testing.T
}

func (tl *testLogger) logLevel(lvl, msg string, keyvals ...interface{}) {
	if tl.t == nil {
		return
	}
	args := []interface{}{lvl, msg}
	args = append(args, keyvals...)
	tl.t.Log(args...)
}

func (tl *testLogger) Debug(msg string, keyvals ...interface{}) {
	tl.logLevel("DEBUG", msg, keyvals)
}

func (tl *testLogger) Info(msg string, keyvals ...interface{}) {
	tl.logLevel("INFO ", msg, keyvals)
}

func (tl *testLogger) Warn(msg string, keyvals ...interface{}) {
	tl.logLevel("WARN ", msg, keyvals)
}

func (tl *testLogger) Error(msg string, keyvals ...interface{}) {
	tl.logLevel("ERROR", msg, keyvals)
}
