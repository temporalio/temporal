package stream

import "go.temporal.io/api/serviceerror"

// Small wrappers so publish.go can reference the serviceerror constructors
// from package-scope `var` initializers without an import cycle concern.

func serviceerrorInvalidArgument(msg string) error {
	return serviceerror.NewInvalidArgument(msg)
}

func serviceerrorNotFound(msg string) error {
	return serviceerror.NewNotFound(msg)
}
