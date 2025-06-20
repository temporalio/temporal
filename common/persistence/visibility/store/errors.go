package store

import (
	"strings"

	"go.temporal.io/api/serviceerror"
)

func NewVisibilityStoreInvalidValuesError(errs []error) error {
	var sb strings.Builder
	sb.WriteString("Visibility store validation errors: ")
	for i, err := range errs {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("[")
		sb.WriteString(err.Error())
		sb.WriteString("]")
	}

	return serviceerror.NewInvalidArgument(sb.String())
}
