package config

import (
	"fmt"
	"net/http"
	"sync/atomic"

	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"

	// DO NOT REMOVE THE LINE BELOW
	_ "net/http/pprof"
)

type (
	// PProfInitializerImpl initialize the pprof based on config
	PProfInitializerImpl struct {
		PProf  *PProf
		Logger log.Logger
	}
)

const (
	pprofNotInitialized int32 = 0
	pprofInitialized    int32 = 1
)

// the pprof should only be initialized once per process
// otherwise, the caller / worker will experience weird issue
var pprofStatus = pprofNotInitialized

// NewInitializer create a new instance of PProf Initializer
func (cfg *PProf) NewInitializer(logger log.Logger) *PProfInitializerImpl {
	return &PProfInitializerImpl{
		PProf:  cfg,
		Logger: logger,
	}
}

// Start the pprof based on config
func (initializer *PProfInitializerImpl) Start() error {
	port := initializer.PProf.Port
	if port == 0 {
		initializer.Logger.Info("PProf not started due to port not set")
		return nil
	}

	if atomic.CompareAndSwapInt32(&pprofStatus, pprofNotInitialized, pprofInitialized) {
		go func() {
			initializer.Logger.Info("PProf listen on ", tag.Port(port))
			err := http.ListenAndServe(fmt.Sprintf("localhost:%d", port), nil)
			if err != nil {
				initializer.Logger.Error("listen and serve err", tag.Error(err))
			}
		}()
	}
	return nil
}
