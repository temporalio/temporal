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

package telemetry

import (
	"fmt"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	logtag "go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
)

type (
	// LogKey is a type wrapper for the key part of a common/log/tag.Tag that
	// can be used to create full common/log/tag.Tag instances. It is intended
	// to be used as a const global and shared throughout the codebase.
	LogKey string

	// MetricsKey is a type wrapper for the key part of a common/metrics.Tag that
	// can be used to create full common/metrics.Tag instances. It is intended
	// to be used as a const global and shared throughout the codebase.
	MetricsKey string

	// Key is a type that brings some consistency to the server's use of log,
	// trace, and metrics annotations. The three tagging systems currently in
	// use are not compatible so we provide this type to at least normalize
	// naming. Consumers are expected to create package-global Key instances and
	// then use those instances to create values as appropriate to the context.
	Key struct {
		OTEL    attribute.Key
		Log     LogKey
		Metrics MetricsKey
	}

	mtag struct {
		k, v string
	}
)

func (t mtag) Key() string   { return t.k }
func (t mtag) Value() string { return t.v }

// NewKey creates a new Key instance, inserting the "io.temporal." prefix for
// OTEL attributes if it is absent.
func NewKey(str string) Key {
	fqstr := str
	if !strings.HasPrefix(str, "io.temporal") {
		fqstr = fmt.Sprintf("io.temporal.%s", str)
	}
	return Key{
		OTEL:    attribute.Key(fqstr),
		Log:     LogKey(str),
		Metrics: MetricsKey(str),
	}
}

// String creates a new string-valued logging tag
func (k LogKey) String(v string) logtag.Tag {
	return logtag.NewStringTag(string(k), v)
}

// Int creates a new int-valued logging tag
func (k LogKey) Int(v int) logtag.Tag {
	return logtag.NewInt(string(k), v)
}

// String creates a new string-valued metrics tag
func (k MetricsKey) String(v string) metrics.Tag {
	return mtag{k: string(k), v: v}
}
