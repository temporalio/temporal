package common

type (
	// PProfInitializer initialize the pprof based on config
	PProfInitializer interface {
		Start() error
	}
)
