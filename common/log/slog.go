// The MIT License
//
// Copyright (c) 2023 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2023 Uber Technologies, Inc.
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

// Part of this implementation was taken from https://github.com/uber-go/zap/blob/99f1811d5d2a52264a9c82505a74c2709b077a73/exp/zapslog/handler.go

package log

import (
	"context"
	"log/slog"

	"go.temporal.io/server/common/log/tag"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type handler struct {
	zapLogger *zap.Logger
	logger    Logger
	tags      []tag.Tag
	group     string
}

var _ slog.Handler = (*handler)(nil)

// NewSlogLogger creates an slog.Logger from a given logger.
func NewSlogLogger(logger Logger) *slog.Logger {
	logger = withIncreasedSkip(logger, 3)
	return slog.New(&handler{logger: logger, zapLogger: extractZapLogger(logger), group: "", tags: nil})
}

// Enabled reports whether the handler handles records at the given level.
func (h *handler) Enabled(_ context.Context, level slog.Level) bool {
	if h.zapLogger == nil {
		return true
	}
	return h.zapLogger.Core().Enabled(convertSlogToZapLevel(level))
}

// Handle implements slog.Handler.
func (h *handler) Handle(_ context.Context, record slog.Record) error {
	tags := make([]tag.Tag, len(h.tags), len(h.tags)+record.NumAttrs())
	copy(tags, h.tags)
	record.Attrs(func(attr slog.Attr) bool {
		tags = append(tags, tag.NewZapTag(convertAttrToField(h.prependGroup(attr))))
		return true
	})
	// Not capturing the log location and stack trace here. We seem to not need this functionality since our zapLogger
	// adds the logging-call-at tag.
	switch record.Level {
	case slog.LevelDebug:
		h.logger.Debug(record.Message, tags...)
	case slog.LevelInfo:
		h.logger.Info(record.Message, tags...)
	case slog.LevelWarn:
		h.logger.Warn(record.Message, tags...)
	case slog.LevelError:
		h.logger.Error(record.Message, tags...)
	default:
	}
	return nil
}

// WithAttrs implements slog.Handler.
func (h *handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	tags := make([]tag.Tag, len(h.tags), len(h.tags)+len(attrs))
	copy(tags, h.tags)
	for _, attr := range attrs {
		tags = append(tags, tag.NewZapTag(convertAttrToField(h.prependGroup(attr))))
	}
	return &handler{logger: h.logger, tags: tags, group: h.group}
}

// WithGroup implements slog.Handler.
func (h *handler) WithGroup(name string) slog.Handler {
	group := name
	if h.group != "" {
		group = h.group + "." + name
	}
	return &handler{logger: h.logger, tags: h.tags, group: group}
}

func (h *handler) prependGroup(attr slog.Attr) slog.Attr {
	if h.group == "" {
		return attr
	}
	return slog.Attr{Key: h.group + "." + attr.Key, Value: attr.Value}
}

func extractZapLogger(logger Logger) *zap.Logger {
	switch l := logger.(type) {
	case *zapLogger:
		return l.zl
	case *throttledLogger:
		return extractZapLogger(l.logger)
	case *withLogger:
		return extractZapLogger(l.logger)
	}
	return nil
}

// withIncreasedSkip increases the skip level for the given logger if it embeds a zapLogger.
func withIncreasedSkip(logger Logger, skip int) Logger {
	switch l := logger.(type) {
	case *zapLogger:
		return l.Skip(skip)
	case *throttledLogger:
		return &throttledLogger{
			limiter: l.limiter,
			logger:  withIncreasedSkip(l.logger, skip),
		}
	case *withLogger:
		return &withLogger{
			tags:   l.tags,
			logger: withIncreasedSkip(l.logger, skip),
		}
	}
	return nil
}

// convertSlogToZapLevel maps slog Levels to zap Levels.
// Note that there is some room between slog levels while zap levels are continuous, so we can't 1:1 map them.
// See also https://go.googlesource.com/proposal/+/master/design/56345-structured-logging.md?pli=1#levels
func convertSlogToZapLevel(l slog.Level) zapcore.Level {
	switch {
	case l >= slog.LevelError:
		return zapcore.ErrorLevel
	case l >= slog.LevelWarn:
		return zapcore.WarnLevel
	case l >= slog.LevelInfo:
		return zapcore.InfoLevel
	default:
		return zapcore.DebugLevel
	}
}

// groupObject holds all the Attrs saved in a slog.GroupValue.
type groupObject []slog.Attr

func (gs groupObject) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	for _, attr := range gs {
		convertAttrToField(attr).AddTo(enc)
	}
	return nil
}

func convertAttrToField(attr slog.Attr) zapcore.Field {
	if attr.Equal(slog.Attr{}) {
		// Ignore empty attrs.
		return zap.Skip()
	}

	switch attr.Value.Kind() {
	case slog.KindBool:
		return zap.Bool(attr.Key, attr.Value.Bool())
	case slog.KindDuration:
		return zap.Duration(attr.Key, attr.Value.Duration())
	case slog.KindFloat64:
		return zap.Float64(attr.Key, attr.Value.Float64())
	case slog.KindInt64:
		return zap.Int64(attr.Key, attr.Value.Int64())
	case slog.KindString:
		return zap.String(attr.Key, attr.Value.String())
	case slog.KindTime:
		return zap.Time(attr.Key, attr.Value.Time())
	case slog.KindUint64:
		return zap.Uint64(attr.Key, attr.Value.Uint64())
	case slog.KindGroup:
		return zap.Object(attr.Key, groupObject(attr.Value.Group()))
	case slog.KindLogValuer:
		return convertAttrToField(slog.Attr{
			Key: attr.Key,
			// TODO: resolve the value in a lazy way.
			// This probably needs a new Zap field type
			// that can be resolved lazily.
			Value: attr.Value.Resolve(),
		})
	default:
		return zap.Any(attr.Key, attr.Value.Any())
	}
}
