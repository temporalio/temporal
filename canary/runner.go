// Copyright (c) 2019 Uber Technologies, Inc.
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

package canary

import (
	"sync"

	"go.uber.org/zap"
)

type canaryRunner struct {
	*RuntimeContext
	config *Config
}

// New creates and returns a runnable which spins
// up a set of canaries based on supplied config
func newCanaryRunner(cfg *Config, runtime *RuntimeContext) Runnable {
	return &canaryRunner{
		RuntimeContext: runtime,
		config:         cfg,
	}
}

// Run runs the canaries
func (r *canaryRunner) Run() error {
	r.metrics.Counter("restarts").Inc(1)
	if len(r.config.Excludes) != 0 {
		updateSanityChildWFList(r.config.Excludes)
	}

	var wg sync.WaitGroup
	for _, d := range r.config.Domains {
		canary := newCanary(d, r.RuntimeContext)
		r.logger.Info("starting canary", zap.String("domain", d))
		r.execute(canary, &wg)
	}
	wg.Wait()
	return nil
}

func (r *canaryRunner) execute(task Runnable, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		task.Run()
		wg.Done()
	}()
}

func updateSanityChildWFList(excludes []string) {
	var temp []string
	for _, childName := range sanityChildWFList {
		if !isStringInList(childName, excludes) {
			temp = append(temp, childName)
		}
	}
	sanityChildWFList = temp
}

func isStringInList(str string, list []string) bool {
	for _, l := range list {
		if l == str {
			return true
		}
	}
	return false
}
