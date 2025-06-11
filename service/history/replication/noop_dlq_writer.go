package replication

import "context"

// NoopDLQWriter is a DLQWriter that does nothing. The zero value is a valid instance.
type NoopDLQWriter struct {
}

var _ DLQWriter = NoopDLQWriter{}

func (w NoopDLQWriter) WriteTaskToDLQ(context.Context, DLQWriteRequest) error {
	return nil
}
