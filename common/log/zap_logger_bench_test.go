package log

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"

	"go.temporal.io/server/common/log/tag"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

/**
$ go test -v -bench=. | grep -E "(Bench)|(ns/op)"
BenchmarkZapLoggerWithFields
BenchmarkZapLoggerWithFields-4            152793              7200 ns/op
BenchmarkLoggerWithFields
BenchmarkLoggerWithFields-4               146850              8370 ns/op
BenchmarkZapLoggerWithoutFields
BenchmarkZapLoggerWithoutFields-4         192972              5885 ns/op
BenchmarkLoggerWithoutFields
BenchmarkLoggerWithoutFields-4            162109              7211 ns/op
*/

func BenchmarkZapLoggerWithFields(b *testing.B) {
	zLogger := buildZapLogger(Config{Level: "info"}, false)

	for i := 0; i < b.N; i++ {
		zLoggerWith := zLogger.With(zap.Int64("wf-schedule-id", int64(i)), zap.String("cluster-name", "this is a very long value: 1234567890 1234567890 1234567890 1234567890"))
		zLoggerWith.Info("msg to print log, 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890",
			zap.String("wf-namespace", "test-namespace"))
		zLoggerWith.Debug("msg NOT to print log, 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890",
			zap.String("wf-namespace", "test-namespace"))
	}
}

func BenchmarkLoggerWithFields(b *testing.B) {
	logger := NewZapLogger(buildZapLogger(Config{Level: "info"}, true))

	for i := 0; i < b.N; i++ {
		loggerWith := logger.With(tag.WorkflowScheduledEventID(int64(i)), tag.ClusterName("this is a very long value: 1234567890 1234567890 1234567890 1234567890"))
		loggerWith.Info("msg to print log, 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890",
			tag.WorkflowNamespace("test-namespace"))
		loggerWith.Debug("msg NOT to print log, 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890",
			tag.WorkflowNamespace("test-namespace"))
	}
}

func BenchmarkZapLoggerWithoutFields(b *testing.B) {
	zLogger := buildZapLogger(Config{Level: "info"}, false)

	for i := 0; i < b.N; i++ {
		zLogger.Info("msg to print log, 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890",
			zap.Int64("wf-schedule-id", int64(i)), zap.String("cluster-name", "this is a very long value: 1234567890 1234567890 1234567890 1234567890"),
			zap.String("wf-namespace", "test-namespace"))
		zLogger.Debug("msg NOT to print log, 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890",
			zap.Int64("wf-schedule-id", int64(i)), zap.String("cluster-name", "this is a very long value: 1234567890 1234567890 1234567890 1234567890"),
			zap.String("wf-namespace", "test-namespace"))
	}
}

func BenchmarkLoggerWithoutFields(b *testing.B) {
	logger := NewZapLogger(buildZapLogger(Config{Level: "info"}, true))

	for i := 0; i < b.N; i++ {
		logger.Info("msg to print log, 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890",
			tag.WorkflowNamespace("test-namespace"),
			tag.WorkflowScheduledEventID(int64(i)), tag.ClusterName("this is a very long value: 1234567890 1234567890 1234567890 1234567890"))
		logger.Debug("msg NOT to print log, 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890",
			tag.WorkflowNamespace("test-namespace"),
			tag.WorkflowScheduledEventID(int64(i)), tag.ClusterName("this is a very long value: 1234567890 1234567890 1234567890 1234567890"))
	}
}

type lazyLoggerBenchmarkInput struct {
	namespaceID string
	namespace   string
	workflowID  string
	runID       string
}

type lazyLoggerBenchmarkState struct {
	context      Logger
	throttled    Logger
	mutableState Logger
}

var (
	lazyLoggerBenchmarkConstructionSink lazyLoggerBenchmarkState
	lazyLoggerBenchmarkBaseSink         []Logger
	lazyLoggerBenchmarkInputSink        []lazyLoggerBenchmarkInput
	lazyLoggerBenchmarkRetainedSink     []lazyLoggerBenchmarkState
	lazyLoggerBenchmarkStartedTime      = time.Unix(1700000000, 0)
)

// BenchmarkLazyLoggerConstruction measures the logger shape retained by a
// workflow context and its mutable state. Inputs and base loggers are shared.
func BenchmarkLazyLoggerConstruction(b *testing.B) {
	for _, mode := range []string{"NoLog", "SuppressedDebug", "EnabledInfo"} {
		b.Run(mode, func(b *testing.B) {
			inputs := newLazyLoggerBenchmarkInputs(4096)
			base, throttled := newLazyLoggerBenchmarkBases()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				state := newLazyLoggerBenchmarkState(base, throttled, inputs[i%len(inputs)])
				switch mode {
				case "NoLog":
				case "SuppressedDebug":
					lazyLoggerBenchmarkDebug(state.mutableState)
				case "EnabledInfo":
					state.mutableState.Info("workflow logger benchmark")
				default:
					b.Fatalf("unknown mode %q", mode)
				}
				lazyLoggerBenchmarkConstructionSink = state
			}
		})
	}
}

// BenchmarkLazyLoggerRepeatedDebug measures disabled Debug calls after one
// call has completed, with and without earlier Info materialization.
func BenchmarkLazyLoggerRepeatedDebug(b *testing.B) {
	for _, mode := range []string{"AfterDebug", "AfterInfo"} {
		b.Run(mode, func(b *testing.B) {
			base, throttled := newLazyLoggerBenchmarkBases()
			state := newLazyLoggerBenchmarkState(base, throttled, newLazyLoggerBenchmarkInputs(1)[0])
			if mode == "AfterInfo" {
				state.mutableState.Info("workflow logger benchmark")
			}
			lazyLoggerBenchmarkDebug(state.mutableState)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				lazyLoggerBenchmarkDebug(state.mutableState)
			}
			b.StopTimer()
			runtime.KeepAlive(state)
		})
	}
}

// BenchmarkLazyLoggerRetained is opt-in because it retains hundreds of
// thousands of workflow logger triples. Run one sub-benchmark per fresh process
// with TEMPORAL_LAZY_LOGGER_RETAINED_BENCH=1 and -test.benchtime=1x. Use a
// separate memory-profile run; its timing is not a CPU benchmark result.
func BenchmarkLazyLoggerRetained(b *testing.B) {
	if os.Getenv("TEMPORAL_LAZY_LOGGER_RETAINED_BENCH") != "1" {
		b.Skip("set TEMPORAL_LAZY_LOGGER_RETAINED_BENCH=1 and use -test.benchtime=1x")
	}
	for _, count := range []int{25000, 100000, 250000} {
		b.Run("SuppressedDebug/"+strconv.Itoa(count), func(b *testing.B) {
			benchmarkLazyLoggerRetained(b, count, "SuppressedDebug")
		})
	}
	for _, mode := range []string{"NoLog", "DebugThenInfo10Percent"} {
		b.Run(mode+"/250000", func(b *testing.B) {
			benchmarkLazyLoggerRetained(b, 250000, mode)
		})
	}
}

func benchmarkLazyLoggerRetained(b *testing.B, count int, mode string) {
	if b.N != 1 {
		b.Fatal("retained benchmark requires -test.benchtime=1x")
	}
	inputs := newLazyLoggerBenchmarkInputs(count)
	states := make([]lazyLoggerBenchmarkState, count)
	base, throttled := newLazyLoggerBenchmarkBases()
	lazyLoggerBenchmarkBaseSink = []Logger{base, throttled}
	lazyLoggerBenchmarkInputSink = inputs
	lazyLoggerBenchmarkRetainedSink = states
	//nolint:revive // Retained heap measurement requires explicit garbage collection.
	runtime.GC()
	//nolint:revive // Retained heap measurement requires explicit garbage collection.
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	b.ResetTimer()
	for i, input := range inputs {
		state := newLazyLoggerBenchmarkState(base, throttled, input)
		switch mode {
		case "NoLog":
		case "SuppressedDebug":
			lazyLoggerBenchmarkDebug(state.mutableState)
		case "DebugThenInfo10Percent":
			lazyLoggerBenchmarkDebug(state.mutableState)
			if i%10 == 0 {
				state.mutableState.Info("workflow logger benchmark")
			}
		default:
			b.Fatalf("unknown mode %q", mode)
		}
		states[i] = state
	}
	b.StopTimer()
	//nolint:revive // Retained heap measurement requires explicit garbage collection.
	runtime.GC()
	//nolint:revive // Retained heap measurement requires explicit garbage collection.
	runtime.GC()
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(inputs)
	runtime.KeepAlive(states)
	b.ReportMetric(float64(count), "workflows/op")
	b.ReportMetric(float64(int64(after.HeapAlloc)-int64(before.HeapAlloc))/float64(count), "retained-B/workflow")
	b.ReportMetric(float64(int64(after.HeapObjects)-int64(before.HeapObjects))/float64(count), "retained-objects/workflow")
}

func newLazyLoggerBenchmarkBases() (base Logger, throttled Logger) {
	level := zap.NewAtomicLevelAt(zap.InfoLevel)
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(DefaultZapEncoderConfig),
		zapcore.AddSync(io.Discard),
		level,
	)
	base = With(NewZapLoggerWithLazyDebugSuppression(zap.New(core)), tag.String("service", "history"), tag.ClusterName("tem8"))
	throttled = NewThrottledLogger(base, func() float64 { return 1000 })
	return base, throttled
}

func newLazyLoggerBenchmarkInputs(count int) []lazyLoggerBenchmarkInput {
	inputs := make([]lazyLoggerBenchmarkInput, count)
	for i := range inputs {
		inputs[i] = lazyLoggerBenchmarkInput{
			namespaceID: fmt.Sprintf("00000000-0000-0000-0000-%012x", i%100),
			namespace:   fmt.Sprintf("namespace-%03d", i%100),
			workflowID:  fmt.Sprintf("workflow-%056d", i),
			runID:       fmt.Sprintf("10000000-0000-0000-0000-%012x", i),
		}
	}
	return inputs
}

func newLazyLoggerBenchmarkState(base, throttled Logger, input lazyLoggerBenchmarkInput) lazyLoggerBenchmarkState {
	namespaceID, workflowID, runID := input.namespaceID, input.workflowID, input.runID
	contextTags := func() []tag.Tag {
		return []tag.Tag{
			tag.WorkflowNamespaceID(namespaceID),
			tag.WorkflowID(workflowID),
			tag.WorkflowRunID(runID),
		}
	}
	contextLogger := NewLazyLogger(base, contextTags)
	throttledLogger := NewLazyLogger(throttled, contextTags)
	namespace := input.namespace
	mutableStateLogger := NewLazyLogger(contextLogger, func() []tag.Tag {
		return []tag.Tag{
			tag.WorkflowNamespace(namespace),
			tag.WorkflowID(workflowID),
			tag.WorkflowRunID(runID),
		}
	})
	return lazyLoggerBenchmarkState{
		context:      contextLogger,
		throttled:    throttledLogger,
		mutableState: mutableStateLogger,
	}
}

func lazyLoggerBenchmarkDebug(logger Logger) {
	logger.Debug("Workflow task updated",
		tag.WorkflowScheduledEventID(12),
		tag.WorkflowStartedEventID(13),
		tag.WorkflowTaskRequestId("20000000-0000-0000-0000-000000000001"),
		tag.WorkflowTaskTimeout(10*time.Second),
		tag.Attempt(1),
		tag.WorkflowStartedTimestamp(lazyLoggerBenchmarkStartedTime),
		tag.WorkflowTaskType("Normal"))
}
