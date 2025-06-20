package common

import "go.uber.org/fx"

// WorkerComponentTag is the fx group tag for worker components. This is used to allow those who use Temporal as a
// library to dynamically register their own system workers. Use this to annotate a worker component consumer. Use the
// AnnotateWorkerComponentProvider function to annotate a worker component provider.
const WorkerComponentTag = `group:"workerComponent"`

// AnnotateWorkerComponentProvider converts a WorkerComponent factory function into an fx provider which will add the
// WorkerComponentTag to the result.
func AnnotateWorkerComponentProvider[T any](f func(t T) WorkerComponent) fx.Option {
	return fx.Provide(fx.Annotate(f, fx.ResultTags(WorkerComponentTag)))
}
