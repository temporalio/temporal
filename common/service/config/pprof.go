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
	// DO NOT REMOVE THE LINE BELOW
	_ "net/http/pprof"

	"github.com/uber-common/bark"
)

type (
	// PProfInitializerImpl initialize the pprof based on config
	PProfInitializerImpl struct {
		PProf  *PProf
		Logger bark.Logger
	}
)

// NewInitializer create a new instance of PProf Initializer
func (cfg *PProf) NewInitializer(logger bark.Logger) *PProfInitializerImpl {
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

	go func() {
		initializer.Logger.Info("PProf listen on %d", port)
		http.ListenAndServe(fmt.Sprintf("localhost:%d", port), nil)
	}()

	return nil
}
