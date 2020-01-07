// Copyright (c) 2017 Uber Technologies, Inc.
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

package config

import (
	"fmt"
	"net/http"
	"sync/atomic"

	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"

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
