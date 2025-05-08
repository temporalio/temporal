//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination interface_mock.go

package log

import (
	"go.temporal.io/server/common/log/tag"
)

type (
	// Logger is the logging interface.
	// Usage example:
	//  logger.Info("hello world",
	//          tag.WorkflowNextEventID(123),
	//          tag.WorkflowActionWorkflowStarted,
	//          tag.WorkflowNamespaceID("test-namespace-id")),
	//	 )
	//  Note: msg should be static, do not use fmt.Sprintf() for msg. Anything dynamic should be tagged.
	Logger interface {
		Debug(msg string, tags ...tag.Tag)
		Info(msg string, tags ...tag.Tag)
		Warn(msg string, tags ...tag.Tag)
		Error(msg string, tags ...tag.Tag)
		DPanic(msg string, tags ...tag.Tag)
		Panic(msg string, tags ...tag.Tag)
		Fatal(msg string, tags ...tag.Tag)
	}

	// Implement WithLogger interface with With method which should return new instance of logger with prepended tags.
	// If WithLogger is not implemented on logger, internal (not very efficient) preppender is used.
	// Create prepended logger example:
	//  logger = log.With(
	//          logger,
	//          tag.WorkflowNextEventID(123),
	//          tag.WorkflowActionWorkflowStarted,
	//          tag.WorkflowNamespaceID("test-namespace-id"))
	//  ...
	//  logger.Info("hello world")
	WithLogger interface {
		With(tags ...tag.Tag) Logger
	}

	// If logger implements SkipLogger then Skip method will be called and extraSkip parameter will have
	// number of extra stack trace frames to skip (useful to log caller func file/line).
	SkipLogger interface {
		Skip(extraSkip int) Logger
	}

	// Special logger types for use with fx
	SnTaggedLogger  Logger
	ThrottledLogger Logger
)
