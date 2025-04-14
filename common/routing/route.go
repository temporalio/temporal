// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

// Package routing provides utilities to define a number of [Route] instances, which can be...
//
//  1. Used by servers to...
//
//     1.a. Register with [github.com/gorilla/mux.Router] instances via the [Route.Representation] method.
//
//     1.b. Deserialize HTTP path variables into a struct with the [Route.Deserialize] method.
//
//  2. Used by clients to construct HTTP paths for requests by calling the [Route.Path] method.
package routing

import (
	"net/url"
	"strings"
)

// Route represents a series of HTTP path components.
type Route[T any] struct {
	components []Component[T]
}

// Component represents a single HTTP path component, either a constant slug or a variable parameter.
type Component[T any] interface {
	// Representation is the string representation of the component for usage in a path definition, e.g. "v1" for a
	// constant slug or "{namespace}" for a variable. This should be compatible with the format specified in
	// the [github.com/gorilla/mux] package.
	Representation() string
	// Serialize returns the actual value of the slug when used in an HTTP path, e.g. "v1" for a constant slug or
	// "test-namespace" for a variable.
	Serialize(params T) string
	// Deserialize mutates the given params object with the value of the component when parsing an HTTP path, e.g.
	// setting the value of a variable to "test-namespace". If the component is a constant slug, this method should
	// be a no-op.
	Deserialize(vars map[string]string, t *T)
}

// NewRoute returns a new [Route] instance with the given components.
func NewRoute[T any](components ...Component[T]) Route[T] {
	return Route[T]{components: components}
}

// RouteBuilder is a builder for the [Route] interface.
type RouteBuilder[T any] struct {
	components []Component[T]
}

// NewBuilder creates a new [RouteBuilder] instance, which can be used to define a new [Route] via a fluent API.
func NewBuilder[T any]() *RouteBuilder[T] {
	return &RouteBuilder[T]{}
}

// With adds a series of [Component] instances to the [Route].
func (r *RouteBuilder[T]) With(c ...Component[T]) *RouteBuilder[T] {
	r.components = append(r.components, c...)
	return r
}

// Constant adds a [Constant] component to the [Route].
func (r *RouteBuilder[T]) Constant(values ...string) *RouteBuilder[T] {
	return r.With(Constant[T](values...))
}

// StringVariable adds a [StringVariable] component to the [Route].
func (r *RouteBuilder[T]) StringVariable(name string, getter func(*T) *string) *RouteBuilder[T] {
	return r.With(StringVariable[T](name, getter))
}

// Build returns a read-only [Route].
func (r *RouteBuilder[T]) Build() Route[T] {
	return NewRoute[T](r.components...)
}

// Representation returns the [github.com/gorilla/mux] compatible string representation of the route for usage in a
// path definition. It does not add a leading or trailing slash to the representation, but it won't remove them if
// they're present in the components. We do this because it's easier to add a slash depending on the context than to
// remove it.
func (r Route[T]) Representation() string {
	return r.serialize(func(c Component[T]) string {
		return c.Representation()
	})
}

// Path returns the serialized path of the route with the given params. There will be no leading or trailing slashes,
// similar to the behavior of the [Route.Representation] method.
func (r Route[T]) Path(t T) string {
	return r.serialize(func(c Component[T]) string {
		return c.Serialize(t)
	})
}

func (r Route[T]) serialize(f func(c Component[T]) string) string {
	var sb strings.Builder
	for i, c := range r.components {
		if i > 0 {
			sb.WriteString("/")
		}
		sb.WriteString(f(c))
	}
	return sb.String()
}

// Deserialize the given vars into a new instance of the params type, T.
func (r Route[T]) Deserialize(vars map[string]string) T {
	var t T
	for _, c := range r.components {
		c.Deserialize(vars, &t)
	}
	return t
}

// Constant returns a [Component] that represents a series of constant HTTP path components in a Route.
// They will be joined via strings when used to construct a path or path representation.
func Constant[T any](values ...string) constant[T] {
	return values
}

type constant[T any] []string

func (s constant[T]) Representation() string {
	return strings.Join(s, "/")
}

func (s constant[T]) Serialize(T) string {
	return strings.Join(s, "/")
}

func (s constant[T]) Deserialize(map[string]string, *T) {}

// StringVariable returns a [Component] that represents a string variable in a Route.
func StringVariable[T any](name string, getter func(*T) *string) stringVariable[T] {
	return stringVariable[T]{name, getter}
}

type stringVariable[T any] struct {
	name   string
	getter func(*T) *string
}

func (s stringVariable[T]) Representation() string {
	return "{" + s.name + "}"
}

func (s stringVariable[T]) Serialize(t T) string {
	return url.PathEscape(*s.getter(&t))
}

func (s stringVariable[T]) Deserialize(vars map[string]string, t *T) {
	*s.getter(t) = vars[s.name]
}
