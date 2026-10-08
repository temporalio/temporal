package log

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/tests/testutils"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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

type panickingMarshaler struct{}

func (panickingMarshaler) MarshalJSON() ([]byte, error) {
	panic("boom")
}

func (s *LogSuite) TestReflectedEncoderRecoversFromPanic() {
	dir := testutils.MkdirTemp(s.T(), "", "config.testReflectedEncoder")
	logFile := dir + "/test.log"

	zl := BuildZapLogger(Config{Level: "info", OutputFile: logFile})
	logger := NewZapLogger(zl)

	// A panicking MarshalJSON on a reflected (tag.Any) field must not take down the process.
	s.NotPanics(func() {
		logger.Info("reflected field", tag.Any("key", panickingMarshaler{}))
	})
	s.NoError(zl.Sync())

	out, err := os.ReadFile(logFile)
	s.NoError(err)
	s.Contains(string(out), "panic serializing log field")
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
		tag.String("xray", "alpha"), tag.String("xray", "yankee")) // alpha will never be seen
	withLogger = With(withLogger, tag.String("xray", "zulu"))
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

func TestLazyLoggerDisabledDebugAndLaterInfo(t *testing.T) {
	for _, testCase := range []struct {
		name           string
		wrap           func(Logger) Logger
		withWrapperTag bool
	}{
		{name: "direct", wrap: func(logger Logger) Logger { return logger }},
		{name: "with logger", wrap: func(logger Logger) Logger {
			return newWithLogger(logger, tag.String("wrapper", "present"))
		}, withWrapperTag: true},
		{name: "zap with", wrap: func(logger Logger) Logger {
			return With(logger, tag.String("wrapper", "present"))
		}, withWrapperTag: true},
		{name: "zap skip", wrap: func(logger Logger) Logger {
			return logger.(SkipLogger).Skip(1)
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			base, _, output := newLazyLoggerTestZap()
			innerCalls, outerCalls := 0, 0
			inner := NewLazyLogger(testCase.wrap(base), func() []tag.Tag {
				innerCalls++
				return []tag.Tag{tag.String("inner", "present"), tag.String("duplicate", "inner")}
			})
			outer := NewLazyLogger(inner, func() []tag.Tag {
				outerCalls++
				return []tag.Tag{tag.String("outer", "present"), tag.String("duplicate", "outer")}
			})

			outer.Debug("suppressed")
			require.Zero(t, innerCalls)
			require.Zero(t, outerCalls)
			require.Empty(t, output.String())

			outer.Info("materialized")
			outer.Debug("suppressed after materialization")
			outer.Info("materialized again")
			require.Equal(t, 1, innerCalls)
			require.Equal(t, 1, outerCalls)
			entries := lazyLoggerTestEntries(t, output)
			require.Len(t, entries, 2)
			for _, entry := range entries {
				require.Equal(t, "present", entry["inner"])
				require.Equal(t, "present", entry["outer"])
				require.Equal(t, "outer", entry["duplicate"])
			}
			if testCase.withWrapperTag {
				require.Equal(t, "present", entries[0]["wrapper"])
			}
		})
	}
}

func TestLazyLoggerWithStillMaterializes(t *testing.T) {
	base, _, output := newLazyLoggerTestZap()
	calls := 0
	logger := NewLazyLogger(base, func() []tag.Tag {
		calls++
		return []tag.Tag{tag.String("lazy", "present")}
	})

	require.NotNil(t, logger.With(tag.String("additional", "present")))
	require.Equal(t, 1, calls)
	require.Empty(t, output.String())
}

func TestLazyLoggerDynamicDebugLevel(t *testing.T) {
	for _, materializeWithInfo := range []bool{false, true} {
		t.Run(fmt.Sprintf("info first %t", materializeWithInfo), func(t *testing.T) {
			base, level, output := newLazyLoggerTestZap()
			calls := 0
			logger := NewLazyLogger(base, func() []tag.Tag {
				calls++
				return []tag.Tag{tag.String("lazy", "present")}
			})

			logger.Debug("initially suppressed")
			require.Zero(t, calls)
			expectedMessages := []string{}
			if materializeWithInfo {
				logger.Info("initial info")
				expectedMessages = append(expectedMessages, "initial info")
			}
			level.SetLevel(zap.DebugLevel)
			logger.Debug("enabled debug")
			expectedMessages = append(expectedMessages, "enabled debug")
			level.SetLevel(zap.InfoLevel)
			logger.Debug("suppressed again")
			logger.Info("final info")
			expectedMessages = append(expectedMessages, "final info")

			require.Equal(t, 1, calls)
			entries := lazyLoggerTestEntries(t, output)
			require.Len(t, entries, len(expectedMessages))
			for i, entry := range entries {
				require.Equal(t, expectedMessages[i], entry["msg"])
				require.Equal(t, "present", entry["lazy"])
			}
		})
	}
}

func TestLazyLoggerUnknownLoggerFallback(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		wrap     func(Logger) Logger
		expected []string
	}{
		{
			name:     "direct",
			wrap:     func(logger Logger) Logger { return logger },
			expected: []string{"tags", "with", "debug", "info"},
		},
		{
			name: "with logger",
			wrap: func(logger Logger) Logger {
				return newWithLogger(logger, tag.String("wrapper", "present"))
			},
			expected: []string{"tags", "debug", "info"},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			events := []string{}
			base := &lazyLoggerRecordingLogger{events: &events}
			logger := NewLazyLogger(testCase.wrap(base), func() []tag.Tag {
				events = append(events, "tags")
				return []tag.Tag{tag.String("lazy", "present")}
			})

			logger.Debug("unknown debug behavior")
			logger.Info("info")
			require.Equal(t, testCase.expected, events)
		})
	}
}

func TestLazyLoggerCustomZapCoreFallback(t *testing.T) {
	for _, materializeWithInfo := range []bool{false, true} {
		t.Run(fmt.Sprintf("info first %t", materializeWithInfo), func(t *testing.T) {
			output := &bytes.Buffer{}
			newCore := func(level zapcore.LevelEnabler) zapcore.Core {
				return zapcore.NewCore(
					zapcore.NewJSONEncoder(DefaultZapEncoderConfig),
					zapcore.AddSync(output),
					level,
				)
			}
			withCalls := 0
			core := &lazyLoggerDebugOnWithCore{
				Core:      newCore(zap.InfoLevel),
				debugCore: newCore(zap.DebugLevel),
				withCalls: &withCalls,
			}
			calls := 0
			logger := NewLazyLogger(NewZapLogger(zap.New(core)), func() []tag.Tag {
				calls++
				return []tag.Tag{tag.String("lazy", "present")}
			})

			expectedMessages := []string{}
			if materializeWithInfo {
				logger.Info("info")
				expectedMessages = append(expectedMessages, "info")
			}
			logger.Debug("debug enabled by with")
			expectedMessages = append(expectedMessages, "debug enabled by with")

			require.Equal(t, 1, calls)
			require.Equal(t, 1, withCalls)
			entries := lazyLoggerTestEntries(t, output)
			require.Len(t, entries, len(expectedMessages))
			for i, entry := range entries {
				require.Equal(t, expectedMessages[i], entry["msg"])
				require.Equal(t, "present", entry["lazy"])
			}
		})
	}
}

func TestLazyLoggerDisabledDebugDefersTagSnapshot(t *testing.T) {
	base, _, output := newLazyLoggerTestZap()
	value := "before"
	logger := NewLazyLogger(base, func() []tag.Tag {
		return []tag.Tag{tag.String("snapshot", value)}
	})

	logger.Debug("suppressed")
	value = "after"
	logger.Info("materialized")
	entries := lazyLoggerTestEntries(t, output)
	require.Len(t, entries, 1)
	require.Equal(t, "after", entries[0]["snapshot"])
}

func TestLazyLoggerConcurrentDebugAndMaterialization(t *testing.T) {
	base, _, output := newLazyLoggerTestZap()
	var calls atomic.Int32
	logger := NewLazyLogger(base, func() []tag.Tag {
		calls.Add(1)
		return []tag.Tag{tag.String("lazy", "present")}
	})
	start := make(chan struct{})
	var workers sync.WaitGroup
	workers.Add(9)
	go func() {
		defer workers.Done()
		<-start
		logger.Info("materialized")
	}()
	for range 8 {
		go func() {
			defer workers.Done()
			<-start
			for range 1000 {
				logger.Debug("suppressed")
			}
		}()
	}
	close(start)
	workers.Wait()

	require.Equal(t, int32(1), calls.Load())
	entries := lazyLoggerTestEntries(t, output)
	require.Len(t, entries, 1)
	require.Equal(t, "present", entries[0]["lazy"])
}

func newLazyLoggerTestZap() (*zapLogger, zap.AtomicLevel, *bytes.Buffer) {
	output := &bytes.Buffer{}
	level := zap.NewAtomicLevelAt(zap.InfoLevel)
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(DefaultZapEncoderConfig),
		zapcore.Lock(zapcore.AddSync(output)),
		level,
	)
	return NewZapLoggerWithLazyDebugSuppression(zap.New(core)), level, output
}

func lazyLoggerTestEntries(t *testing.T, output *bytes.Buffer) []map[string]any {
	t.Helper()
	if output.Len() == 0 {
		return nil
	}
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	entries := make([]map[string]any, 0, len(lines))
	for _, line := range lines {
		var entry map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &entry))
		entries = append(entries, entry)
	}
	return entries
}

type lazyLoggerRecordingLogger struct {
	events *[]string
}

type lazyLoggerDebugOnWithCore struct {
	zapcore.Core
	debugCore zapcore.Core
	withCalls *int
}

func (c *lazyLoggerDebugOnWithCore) With(fields []zapcore.Field) zapcore.Core {
	(*c.withCalls)++
	return c.debugCore.With(fields)
}

func (l *lazyLoggerRecordingLogger) Debug(string, ...tag.Tag) {
	*l.events = append(*l.events, "debug")
}

func (l *lazyLoggerRecordingLogger) Info(string, ...tag.Tag) {
	*l.events = append(*l.events, "info")
}

func (*lazyLoggerRecordingLogger) Warn(string, ...tag.Tag)   {}
func (*lazyLoggerRecordingLogger) Error(string, ...tag.Tag)  {}
func (*lazyLoggerRecordingLogger) DPanic(string, ...tag.Tag) {}
func (*lazyLoggerRecordingLogger) Panic(string, ...tag.Tag)  {}
func (*lazyLoggerRecordingLogger) Fatal(string, ...tag.Tag)  {}

func (l *lazyLoggerRecordingLogger) With(...tag.Tag) Logger {
	*l.events = append(*l.events, "with")
	return &lazyLoggerRecordingLogger{events: l.events}
}
