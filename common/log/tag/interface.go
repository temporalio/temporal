package tag

import "go.temporal.io/testx/softassert"

// Tag is implemented by custom fields supplied to a logger.
type Tag = softassert.Field
