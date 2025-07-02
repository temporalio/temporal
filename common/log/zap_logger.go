package log

import (
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"

	"go.temporal.io/server/common/log/tag"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	skipForZapLogger = 3
	// we put a default message when it is empty so that the log can be searchable/filterable
	defaultMsgForEmpty = "none"
	// TODO: once `NewTestLogger` has been removed, move these vars into testlogger.TestLogger
	TestLogFormatEnvVar = "TEMPORAL_TEST_LOG_FORMAT" // set to "json" for json logs in tests
	TestLogLevelEnvVar  = "TEMPORAL_TEST_LOG_LEVEL"  // set to "debug" for debug level logs in tests
)

var DefaultZapEncoderConfig = zapcore.EncoderConfig{
	TimeKey:        "ts",
	LevelKey:       "level",
	NameKey:        "logger",
	CallerKey:      zapcore.OmitKey, // we use our own caller
	FunctionKey:    zapcore.OmitKey,
	MessageKey:     "msg",
	StacktraceKey:  "stacktrace",
	LineEnding:     zapcore.DefaultLineEnding,
	EncodeLevel:    zapcore.LowercaseLevelEncoder,
	EncodeTime:     zapcore.ISO8601TimeEncoder,
	EncodeDuration: zapcore.SecondsDurationEncoder,
	EncodeCaller:   zapcore.ShortCallerEncoder,
}

type (
	// zapLogger is logger backed up by zap.Logger.
	zapLogger struct {
		zl     *zap.Logger
		skip   int
		baseZl *zap.Logger // original, without tags, for cloning new tagged versions, because Zap doesn't dedupe :(
		tags   []tag.Tag   // my tags, for passing to clones
	}
)

var _ Logger = (*zapLogger)(nil)

// NewTestLogger returns a logger for tests
// Deprecated: Use testlogger.TestLogger instead.
func NewTestLogger() *zapLogger {
	format := os.Getenv(TestLogFormatEnvVar)
	if format == "" {
		format = "console"
	}

	logger := BuildZapLogger(Config{
		Level:       os.Getenv(TestLogLevelEnvVar),
		Format:      format,
		Development: true,
	})

	// Don't include stack traces for warnings during tests. Only include them for logs with level error and above.
	logger = logger.WithOptions(zap.AddStacktrace(zap.ErrorLevel))

	return NewZapLogger(logger)
}

// NewCLILogger returns a logger at debug level and log into STDERR for logging from within CLI tools
func NewCLILogger() *zapLogger {
	return NewZapLogger(buildCLIZapLogger())
}

// NewZapLogger returns a new zap based logger from zap.Logger
func NewZapLogger(zl *zap.Logger) *zapLogger {
	return &zapLogger{
		zl:     zl,
		skip:   skipForZapLogger,
		baseZl: zl,
	}
}

// BuildZapLogger builds and returns a new zap.Logger for this logging configuration
func BuildZapLogger(cfg Config) *zap.Logger {
	return buildZapLogger(cfg, true)
}

func caller(skip int) string {
	_, path, line, ok := runtime.Caller(skip)
	if !ok {
		return ""
	}
	return path + ":" + strconv.Itoa(line)
}

func (l *zapLogger) buildFieldsWithCallAt(tags []tag.Tag) []zap.Field {
	fields := make([]zap.Field, len(tags)+1)
	l.fillFields(tags, fields)
	fields[len(fields)-1] = zap.String(tag.LoggingCallAtKey, caller(l.skip))
	return fields
}

// fillFields fill fields parameter with fields read from tags. Optimized for performance.
func (l *zapLogger) fillFields(tags []tag.Tag, fields []zap.Field) {
	for i, t := range tags {
		if zt, ok := t.(tag.ZapTag); ok {
			fields[i] = zt.Field()
		} else {
			fields[i] = zap.Any(t.Key(), t.Value())
		}
	}
}

func setDefaultMsg(msg string) string {
	if msg == "" {
		return defaultMsgForEmpty
	}
	return msg
}

func (l *zapLogger) Debug(msg string, tags ...tag.Tag) {
	if l.zl.Core().Enabled(zap.DebugLevel) {
		msg = setDefaultMsg(msg)
		fields := l.buildFieldsWithCallAt(tags)
		l.zl.Debug(msg, fields...)
	}
}

func (l *zapLogger) Info(msg string, tags ...tag.Tag) {
	if l.zl.Core().Enabled(zap.InfoLevel) {
		msg = setDefaultMsg(msg)
		fields := l.buildFieldsWithCallAt(tags)
		l.zl.Info(msg, fields...)
	}
}

func (l *zapLogger) Warn(msg string, tags ...tag.Tag) {
	if l.zl.Core().Enabled(zap.WarnLevel) {
		msg = setDefaultMsg(msg)
		fields := l.buildFieldsWithCallAt(tags)
		l.zl.Warn(msg, fields...)
	}
}

func (l *zapLogger) Error(msg string, tags ...tag.Tag) {
	if l.zl.Core().Enabled(zap.ErrorLevel) {
		msg = setDefaultMsg(msg)
		fields := l.buildFieldsWithCallAt(tags)
		l.zl.Error(msg, fields...)
	}
}

func (l *zapLogger) DPanic(msg string, tags ...tag.Tag) {
	if l.zl.Core().Enabled(zap.DPanicLevel) {
		msg = setDefaultMsg(msg)
		fields := l.buildFieldsWithCallAt(tags)
		l.zl.DPanic(msg, fields...)
	}
}

func (l *zapLogger) Panic(msg string, tags ...tag.Tag) {
	if l.zl.Core().Enabled(zap.PanicLevel) {
		msg = setDefaultMsg(msg)
		fields := l.buildFieldsWithCallAt(tags)
		l.zl.Panic(msg, fields...)
	}
}

func (l *zapLogger) Fatal(msg string, tags ...tag.Tag) {
	if l.zl.Core().Enabled(zap.FatalLevel) {
		msg = setDefaultMsg(msg)
		fields := l.buildFieldsWithCallAt(tags)
		l.zl.Fatal(msg, fields...)
	}
}

// With() handles the provided tags as "upserts", replacing any matching keys with new values.
// Note that we distinguish between the following two seemingly identical lines:
//
//	logger.With(logger, tag.NewStringTag("foo", "bar")).Info("msg")
//	logger.Info("msg", tag.NewStringTag("foo", "bar")
//
// by deduping "foo" against any existing "foo" tags *only in the former*
func (l *zapLogger) With(tags ...tag.Tag) Logger {
	cloneTags := mergeTags(l.tags, tags)
	if l.baseZl == nil {
		l.baseZl = l.zl
	}
	return l.cloneWithTags(cloneTags)
}

func (l *zapLogger) cloneWithTags(tags []tag.Tag) Logger {
	fields := make([]zap.Field, len(tags))
	l.fillFields(tags, fields)
	zl := l.baseZl.With(fields...)
	return &zapLogger{
		zl:     zl,
		skip:   l.skip,
		baseZl: l.baseZl,
		tags:   tags,
	}
}

func (l *zapLogger) Skip(extraSkip int) Logger {
	return &zapLogger{
		zl:     l.zl,
		skip:   l.skip + extraSkip,
		baseZl: l.baseZl,
	}
}

func mergeTags(oldTags, newTags []tag.Tag) (outTags []tag.Tag) {
	// Even if oldTags empty, we don't just return newTags because we need to de-dupe it.
	outTags = slices.Clone(oldTags)
	for _, t := range newTags {
		if i := slices.IndexFunc(outTags, func(ti tag.Tag) bool {
			return ti.Key() == t.Key()
		}); i >= 0 {
			outTags[i] = t
		} else {
			outTags = append(outTags, t)
		}
	}
	return outTags
}

func buildZapLogger(cfg Config, disableCaller bool) *zap.Logger {
	encodeConfig := DefaultZapEncoderConfig
	if disableCaller {
		encodeConfig.CallerKey = zapcore.OmitKey
		encodeConfig.EncodeCaller = nil
	}

	outputPath := "stderr"
	if len(cfg.OutputFile) > 0 {
		outputPath = cfg.OutputFile
	}
	if cfg.Stdout {
		outputPath = "stdout"
	}
	encoding := "json"
	if cfg.Format == "console" {
		encoding = "console"
	}
	config := zap.Config{
		Level:            zap.NewAtomicLevelAt(ParseZapLevel(cfg.Level)),
		Development:      cfg.Development,
		Sampling:         nil,
		Encoding:         encoding,
		EncoderConfig:    encodeConfig,
		OutputPaths:      []string{outputPath},
		ErrorOutputPaths: []string{outputPath},
		DisableCaller:    disableCaller,
	}
	logger, _ := config.Build()
	return logger
}

func buildCLIZapLogger() *zap.Logger {
	encodeConfig := zapcore.EncoderConfig{
		TimeKey:        "ts",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      zapcore.OmitKey, // we use our own caller
		FunctionKey:    zapcore.OmitKey,
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalColorLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   nil,
	}

	config := zap.Config{
		Level:             zap.NewAtomicLevelAt(zap.DebugLevel),
		Development:       false,
		DisableStacktrace: os.Getenv("TEMPORAL_CLI_SHOW_STACKS") == "",
		Sampling:          nil,
		Encoding:          "console",
		EncoderConfig:     encodeConfig,
		OutputPaths:       []string{"stderr"},
		ErrorOutputPaths:  []string{"stderr"},
		DisableCaller:     true,
	}
	logger, _ := config.Build()
	return logger
}

func ParseZapLevel(level string) zapcore.Level {
	switch strings.ToLower(level) {
	case "debug":
		return zap.DebugLevel
	case "info":
		return zap.InfoLevel
	case "warn":
		return zap.WarnLevel
	case "error":
		return zap.ErrorLevel
	case "dpanic":
		return zap.DPanicLevel
	case "panic":
		return zap.PanicLevel
	case "fatal":
		return zap.FatalLevel
	default:
		return zap.InfoLevel
	}
}
