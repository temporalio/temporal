package log

import (
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// newZapLogger builds and returns a new zap
// logger for this logging configuration
func newZapLogger(cfg *Config) *zap.Logger {
	encodeConfig := zapcore.EncoderConfig{
		TimeKey:        "ts",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "", // we use our own caller, check common/log/logger.go
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   nil,
	}

	outputPath := "stderr"
	if len(cfg.OutputFile) > 0 {
		outputPath = cfg.OutputFile
		if cfg.Stdout {
			outputPath = "stdout"
		}
	}

	config := zap.Config{
		Level:            zap.NewAtomicLevelAt(parseZapLevel(cfg.Level)),
		Development:      false,
		Sampling:         nil, // consider exposing this to config for our external customer
		Encoding:         "json",
		EncoderConfig:    encodeConfig,
		OutputPaths:      []string{outputPath},
		ErrorOutputPaths: []string{outputPath},
	}
	logger, _ := config.Build()
	return logger
}

func parseZapLevel(level string) zapcore.Level {
	switch strings.ToLower(level) {
	case "debug":
		return zap.DebugLevel
	case "info":
		return zap.InfoLevel
	case "warn":
		return zap.WarnLevel
	case "error":
		return zap.ErrorLevel
	case "fatal":
		return zap.FatalLevel
	default:
		return zap.InfoLevel
	}
}
