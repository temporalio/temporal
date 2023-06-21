// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

package metrics

import (
	"errors"
	"fmt"
	"sync"
)

type (
	// registry tracks a list of metricDefinition objects added with register and then builds a catalog
	// of them using the buildCatalog method. See globalRegistry for more.
	registry struct {
		sync.Mutex
		definitions []metricDefinition
	}
	// catalog is a map of metric name to definition. It should not be modified after it is built.
	catalog map[string]metricDefinition
)

var (
	// globalRegistry tracks metrics defined via the New*Def methods. We use a global variable here so that clients may
	// continue to refer to package-level metrics like metrics.ServiceRequests, while still allowing us to iterate over
	// all metrics defined in the package to register them with the metrics system. The sequence through which metrics
	// are registered, sampled and scraped is as follows:
	//
	// 1. When the metrics package is initialized, statements calling New*Def are executed to define metrics,
	// which adds them metric to the global registry.
	// 2. Before a Handler object is constructed, one this package's fx provider functions will call registry.buildCatalog to
	// buildCatalog the catalog for these metrics.
	// 3. The constructed catalog is passed to the Handler so that it knows the metadata for all defined metrics.
	// 4. Clients call methods on the Handler to obtain metric objects like Handler.Counter and Handler.Timer.
	// 5. Those methods retrieve the metadata from the catalog and use it to construct the metric object using a
	// third-party metrics library, e.g. OpenTelemetry. This is where most of the work happens.
	// 6. Clients record a metric using that metrics object, e.g. by calling CounterFunc, and the sample is recorded.
	// 7. At some point, the /metrics endpoint is scraped, and the Prometheus handler we register will iterate over all
	// the aggregated samples and metrics and write them to the response. The metric metadata we passed to the
	// third-party metrics library in step 5 is used here and rendered in the response as comments like:
	// # HELP <metric name> <metric description>.
	globalRegistry registry
	// errMetricAlreadyExists is returned by registry.buildCatalog when it finds two metrics with the same name.
	errMetricAlreadyExists = errors.New("metric already exists")
)

// register adds a metric definition to the list of pending metric definitions. This method is thread-safe.
func (c *registry) register(name string, opts ...Option) metricDefinition {
	c.Lock()
	defer c.Unlock()

	d := metricDefinition{
		name:        name,
		description: "",
		unit:        "",
	}
	for _, opt := range opts {
		opt.apply(&d)
	}

	c.definitions = append(c.definitions, d)

	return d
}

// buildCatalog builds a catalog from the list of pending metric definitions. It is safe to call this method multiple
// times. This method is thread-safe.
func (c *registry) buildCatalog() (catalog, error) {
	c.Lock()
	defer c.Unlock()

	r := make(catalog, len(c.definitions))
	for _, d := range c.definitions {
		if original, ok := r[d.name]; ok {
			return nil, fmt.Errorf(
				"%w: metric %q already defined with %+v. Cannot redefine with %+v",
				errMetricAlreadyExists, d.name, original, d,
			)
		}

		r[d.name] = d
	}

	return r, nil
}

func (c catalog) getMetric(name string) (metricDefinition, bool) {
	def, ok := c[name]
	return def, ok
}
