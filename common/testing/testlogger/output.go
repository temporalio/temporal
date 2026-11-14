package testlogger

import (
	"cmp"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"go.temporal.io/server/common/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
)

// globalFileCore is a process-wide singleton zapcore.Core that writes to the file specified
// by TEMPORAL_TEST_LOG_FILE. It is a NopCore when the env var is unset.
var (
	globalFileCore     zapcore.Core = zapcore.NewNopCore()
	globalFileCoreOnce sync.Once
)

func getGlobalFileCore() zapcore.Core {
	globalFileCoreOnce.Do(func() {
		logFile := os.Getenv(log.TestLogFileEnvVar)
		if logFile == "" {
			return
		}
		if err := os.MkdirAll(filepath.Dir(logFile), 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "testlogger: failed to create log file dir %s: %v\n", filepath.Dir(logFile), err)
			return
		}
		f, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "testlogger: failed to open log file %s: %v\n", logFile, err)
			return
		}
		format := cmp.Or(os.Getenv(log.TestLogFileFormatEnvVar), "json")
		var enc zapcore.Encoder
		switch strings.ToLower(format) {
		case "console":
			enc = zapcore.NewConsoleEncoder(log.DefaultZapEncoderConfig)
		default: // "json" and anything unrecognized
			enc = zapcore.NewJSONEncoder(log.DefaultZapEncoderConfig)
		}
		level := zapcore.DebugLevel
		if levelV := os.Getenv(log.TestLogFileLevelEnvVar); levelV != "" {
			level = log.ParseZapLevel(levelV)
		}
		globalFileCore = zapcore.NewCore(enc, zapcore.AddSync(f), level)
		fmt.Fprintf(os.Stderr, "testlogger: file logging enabled → %s (format=%s level=%s)\n", logFile, format, level)
	})
	return globalFileCore
}

func (tl *TestLogger) configureLogger(t TestingT) {
	writer := zaptest.NewTestingWriter(t)

	// Console core: format and level controlled by TEMPORAL_TEST_LOG_FORMAT / TEMPORAL_TEST_LOG_LEVEL.
	var consoleEnc zapcore.Encoder
	format := cmp.Or(os.Getenv(log.TestLogFormatEnvVar), "console")
	switch strings.ToLower(format) {
	case "console":
		consoleEnc = zapcore.NewConsoleEncoder(log.DefaultZapEncoderConfig)
	case "json":
		consoleEnc = zapcore.NewJSONEncoder(log.DefaultZapEncoderConfig)
	default:
		t.Fatalf("unknown log encoding %q", format)
	}
	consoleLevel := tl.level
	if levelV := os.Getenv(log.TestLogLevelEnvVar); levelV != "" {
		consoleLevel = log.ParseZapLevel(levelV)
	}
	core := zapcore.NewTee(
		zapcore.NewCore(consoleEnc, writer, consoleLevel),
		getGlobalFileCore())

	zapOptions := []zap.Option{
		zap.ErrorOutput(writer.WithMarkFailed(true)),
		zap.AddStacktrace(zap.ErrorLevel), // only include stack traces for logs with level error and above
		zap.WithCaller(tl.logCaller),
	}

	// Skip(1) skips the shared engine
	tl.wrapped = log.NewZapLogger(zap.New(core, zapOptions...)).Skip(1)
}
