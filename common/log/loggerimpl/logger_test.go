// Copyright (c) 2017 Uber Technologies, Inc.
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

package loggerimpl

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

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

	var zapLogger *zap.Logger
	zapLogger = zap.NewExample()

	logger := NewLogger(zapLogger)
	preCaller := caller(1)
	logger.WithTags(tag.Error(fmt.Errorf("test error"))).Info("test info", tag.WorkflowActionWorkflowStarted)

	// back to normal state
	w.Close()
	os.Stdout = old // restoring the real stdout
	out := <-outC
	sps := strings.Split(preCaller, ":")
	par, err := strconv.Atoi(sps[1])
	assert.Nil(t, err)
	lineNum := fmt.Sprintf("%v", par+1)
	fmt.Println(out, lineNum)
	assert.Equal(t, out, `{"level":"info","msg":"test info","error":"test error","wf-action":"add-workflow-started-event","logging-call-at":"logger_test.go:`+lineNum+`"}`+"\n")

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

	var zapLogger *zap.Logger
	zapLogger = zap.NewExample()

	dc := dynamicconfig.NewNopClient()
	cln := dynamicconfig.NewCollection(dc, NewNopLogger())
	logger := NewThrottledLogger(NewLogger(zapLogger), cln.GetIntProperty(dynamicconfig.FrontendRPS, 1))
	preCaller := caller(1)
	logger.WithTags(tag.Error(fmt.Errorf("test error"))).WithTags(tag.ComponentShard).Info("test info", tag.WorkflowActionWorkflowStarted)

	// back to normal state
	w.Close()
	os.Stdout = old // restoring the real stdout
	out := <-outC
	sps := strings.Split(preCaller, ":")
	par, err := strconv.Atoi(sps[1])
	assert.Nil(t, err)
	lineNum := fmt.Sprintf("%v", par+1)
	fmt.Println(out, lineNum)
	assert.Equal(t, out, `{"level":"info","msg":"test info","error":"test error","component":"shard","wf-action":"add-workflow-started-event","logging-call-at":"logger_test.go:`+lineNum+`"}`+"\n")
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

	var zapLogger *zap.Logger
	zapLogger = zap.NewExample()

	logger := NewLogger(zapLogger)
	preCaller := caller(1)
	logger.WithTags(tag.Error(fmt.Errorf("test error"))).Info("", tag.WorkflowActionWorkflowStarted)

	// back to normal state
	w.Close()
	os.Stdout = old // restoring the real stdout
	out := <-outC
	sps := strings.Split(preCaller, ":")
	par, err := strconv.Atoi(sps[1])
	assert.Nil(t, err)
	lineNum := fmt.Sprintf("%v", par+1)
	fmt.Println(out, lineNum)
	assert.Equal(t, out, `{"level":"info","msg":"`+defaultMsgForEmpty+`","error":"test error","wf-action":"add-workflow-started-event","logging-call-at":"logger_test.go:`+lineNum+`"}`+"\n")

}
