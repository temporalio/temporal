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

	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/service/dynamicconfig"
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
