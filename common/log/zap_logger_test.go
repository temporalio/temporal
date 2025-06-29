package log

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/tests/testutils"
	"go.uber.org/zap"
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
	s.Equal(zap.DebugLevel, ParseZapLevel("debug"))
	s.Equal(zap.InfoLevel, ParseZapLevel("info"))
	s.Equal(zap.WarnLevel, ParseZapLevel("warn"))
	s.Equal(zap.ErrorLevel, ParseZapLevel("error"))
	s.Equal(zap.FatalLevel, ParseZapLevel("fatal"))
	s.Equal(zap.DPanicLevel, ParseZapLevel("dpanic"))
	s.Equal(zap.PanicLevel, ParseZapLevel("panic"))
	s.Equal(zap.InfoLevel, ParseZapLevel("unknown"))
}

func (s *LogSuite) TestNewLogger() {
	dir := testutils.MkdirTemp(s.T(), "", "config.testNewLogger")

	cfg := Config{
		Level:      "info",
		OutputFile: dir + "/test.log",
	}

	log := BuildZapLogger(cfg)
	s.NotNil(log)
	_, err := os.Stat(dir + "/test.log")
	s.Nil(err)
	log.DPanic("Development default is false; should not panic here!")
	s.Panics(nil, func() {
		log.Panic("Must Panic")
	})

	cfg = Config{
		Level:       "info",
		OutputFile:  dir + "/test.log",
		Development: true,
	}
	log = BuildZapLogger(cfg)
	s.NotNil(log)
	_, err = os.Stat(dir + "/test.log")
	s.Nil(err)
	s.Panics(nil, func() {
		log.DPanic("Must panic!")
	})
	s.Panics(nil, func() {
		log.Panic("Must panic!")
	})

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

	// Test tags with duplicate keys are replaced
	withLogger := With(logger,
		tag.NewStringTag("xray", "alpha"), tag.NewStringTag("xray", "yankee")) // alpha will never be seen
	withLogger = With(withLogger, tag.NewStringTag("xray", "zulu"))
	withLogger.Info("Log message with tag")

	// put Stdout back to normal state
	require.Nil(t, w.Close())
	os.Stdout = old // restoring the real stdout
	out := <-outC
	sps := strings.Split(preCaller, ":")
	par, err := strconv.Atoi(sps[1])
	assert.Nil(t, err)
	lineNum := fmt.Sprintf("%v", par+1)
	assert.Regexp(t, `{"level":"info","msg":"test info","error":"test error","wf-action":"add-workflow-started-event","logging-call-at":".*zap_logger_test.go:`+lineNum+`"}`+"\n", out)

	assert.NotRegexp(t, `alpha`, out)  // replaced value
	assert.Regexp(t, `xray`, out)      // key
	assert.NotRegexp(t, `yankee`, out) // replaced value
	assert.Regexp(t, `zulu`, out)      // override value
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
	require.Nil(t, w.Close())
	os.Stdout = old // restoring the real stdout
	out := <-outC
	sps := strings.Split(preCaller, ":")
	par, err := strconv.Atoi(sps[1])
	assert.Nil(t, err)
	lineNum := fmt.Sprintf("%v", par+1)
	fmt.Println(out, lineNum)
	assert.Regexp(t, `{"level":"info","msg":"test info","error":"test error","component":"shard-context","wf-action":"add-workflow-started-event","logging-call-at":".*zap_logger_test.go:`+lineNum+`"}`+"\n", out)
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
	require.Nil(t, w.Close())
	os.Stdout = old // restoring the real stdout
	out := <-outC
	sps := strings.Split(preCaller, ":")
	par, err := strconv.Atoi(sps[1])
	assert.Nil(t, err)
	lineNum := fmt.Sprintf("%v", par+1)
	fmt.Println(out, lineNum)
	assert.Regexp(t, `{"level":"info","msg":"`+defaultMsgForEmpty+`","error":"test error","wf-action":"add-workflow-started-event","logging-call-at":".*zap_logger_test.go:`+lineNum+`"}`+"\n", out)
}
