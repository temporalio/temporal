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

package log

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"

	"go.temporal.io/server/tests/testhelper"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"go.temporal.io/server/common/log/tag"
)

type LogSuite struct {
	*require.Assertions
	suite.Suite
}

func TestLogSuite(t *testing.T) {
	suite.Run(t, new(LogSuite))
}

func (s *LogSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *LogSuite) TestParseLogLevel() {
	s.Equal(zap.DebugLevel, parseZapLevel("debug"))
	s.Equal(zap.InfoLevel, parseZapLevel("info"))
	s.Equal(zap.WarnLevel, parseZapLevel("warn"))
	s.Equal(zap.ErrorLevel, parseZapLevel("error"))
	s.Equal(zap.FatalLevel, parseZapLevel("fatal"))
	s.Equal(zap.InfoLevel, parseZapLevel("unknown"))
}

func (s *LogSuite) TestNewLogger() {
	dir := testhelper.MkdirTemp(s.T(), "", "config.testNewLogger")

	cfg := Config{
		Level:      "info",
		OutputFile: dir + "/test.log",
	}

	log := BuildZapLogger(cfg)
	s.NotNil(log)
	_, err := os.Stat(dir + "/test.log")
	s.Nil(err)
}

func TestDefaultLogger(t *testing.T) {
	old := os.Stdout // keep backup of the real stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	outC := make(chan string)
	// copy the output in a separate goroutine so logging can't block indefinitely
	go func() {
		var buf bytes.Buffer
		_, err := io.Copy(&buf, r)
		assert.NoError(t, err)
		outC <- buf.String()
	}()

	logger := NewZapLogger(zap.NewExample())
	preCaller := caller(1)
	logger.With(tag.Error(fmt.Errorf("test error"))).Info("test info", tag.WorkflowActionWorkflowStarted)

	// back to normal state
	w.Close()
	os.Stdout = old // restoring the real stdout
	out := <-outC
	sps := strings.Split(preCaller, ":")
	par, err := strconv.Atoi(sps[1])
	assert.Nil(t, err)
	lineNum := fmt.Sprintf("%v", par+1)
	fmt.Println(out, lineNum)
	assert.Equal(t, `{"level":"info","msg":"test info","error":"test error","wf-action":"add-workflow-started-event","logging-call-at":"zap_logger_test.go:`+lineNum+`"}`+"\n", out)
}

func TestThrottleLogger(t *testing.T) {
	old := os.Stdout // keep backup of the real stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	outC := make(chan string)
	// copy the output in a separate goroutine so logging can't block indefinitely
	go func() {
		var buf bytes.Buffer
		_, err := io.Copy(&buf, r)
		assert.NoError(t, err)
		outC <- buf.String()
	}()

	logger := NewThrottledLogger(NewZapLogger(zap.NewExample()),
		func() float64 { return 1 })
	preCaller := caller(1)
	With(With(logger, tag.Error(fmt.Errorf("test error"))), tag.ComponentShardContext).Info("test info", tag.WorkflowActionWorkflowStarted)

	// back to normal state
	w.Close()
	os.Stdout = old // restoring the real stdout
	out := <-outC
	sps := strings.Split(preCaller, ":")
	par, err := strconv.Atoi(sps[1])
	assert.Nil(t, err)
	lineNum := fmt.Sprintf("%v", par+1)
	fmt.Println(out, lineNum)
	assert.Equal(t, `{"level":"info","msg":"test info","error":"test error","component":"shard-context","wf-action":"add-workflow-started-event","logging-call-at":"zap_logger_test.go:`+lineNum+`"}`+"\n", out)
}

func TestEmptyMsg(t *testing.T) {
	old := os.Stdout // keep backup of the real stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	outC := make(chan string)
	// copy the output in a separate goroutine so logging can't block indefinitely
	go func() {
		var buf bytes.Buffer
		_, err := io.Copy(&buf, r)
		assert.NoError(t, err)
		outC <- buf.String()
	}()

	logger := NewZapLogger(zap.NewExample())
	preCaller := caller(1)
	logger.With(tag.Error(fmt.Errorf("test error"))).Info("", tag.WorkflowActionWorkflowStarted)

	// back to normal state
	w.Close()
	os.Stdout = old // restoring the real stdout
	out := <-outC
	sps := strings.Split(preCaller, ":")
	par, err := strconv.Atoi(sps[1])
	assert.Nil(t, err)
	lineNum := fmt.Sprintf("%v", par+1)
	fmt.Println(out, lineNum)
	assert.Equal(t, `{"level":"info","msg":"`+defaultMsgForEmpty+`","error":"test error","wf-action":"add-workflow-started-event","logging-call-at":"zap_logger_test.go:`+lineNum+`"}`+"\n", out)
}
