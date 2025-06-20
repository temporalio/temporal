package pprof

import (
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof" // DO NOT REMOVE THE LINE
	"sync/atomic"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

const (
	pprofNotInitialized int32 = 0
	pprofInitialized    int32 = 1
)

type (
	// PProfInitializerImpl initialize the pprof based on config
	PProfInitializerImpl struct {
		PProf  *config.PProf
		Logger log.Logger
	}
)

// the pprof should only be initialized once per process
// otherwise, the caller / worker will experience weird issue
var pprofStatus = pprofNotInitialized

// NewInitializer create a new instance of PProf Initializer
func NewInitializer(cfg *config.PProf, logger log.Logger) *PProfInitializerImpl {
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
	host := initializer.PProf.Host
	if host == "" {
		// default to localhost which will favor ipv4 on dual stack
		// environments - configure host as `::1` to bind on ipv6 localhost
		host = "localhost"
	}

	hostPort := net.JoinHostPort(host, fmt.Sprint(port))

	if atomic.CompareAndSwapInt32(&pprofStatus, pprofNotInitialized, pprofInitialized) {
		go func() {
			initializer.Logger.Info("PProf listen on ", tag.Host(host), tag.Port(port))
			err := http.ListenAndServe(hostPort, nil)
			if err != nil {
				initializer.Logger.Error("listen and serve err", tag.Error(err))
			}
		}()
	}
	return nil
}
