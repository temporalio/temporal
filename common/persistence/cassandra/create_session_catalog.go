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

package cassandra

import (
	"fmt"
	"sync"

	"github.com/gocql/gocql"

	commongocql "go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

// defining a global catalog of "CreateSession" callbacks. It is not
// possible to pass this as a parameter in the config as the config
// is static and matches 1-1 with some YAML definition so a callback
// does not fit in there. By default, uses the upstream gocql
// Cluster.CreateSession implementation.
var createSessionCatalogData map[string]func(func() (*gocql.ClusterConfig, error)) (commongocql.GocqlSession, error)

// locking may not be strictly necessary here, but since this is
// rarely called, the cost is small, and it is still a global
// variable so it can, technically, be subject to race conditions.
var createSessionCatalogMutex sync.RWMutex

// this error never bubbles up, it is fatal, only used for logging
type errorCreateSessionFuncNotRegistered struct {
	name string
}

var _ error = &errorCreateSessionFuncNotRegistered{}

func newErrorCreateSessionFuncNoRegistered(name string) *errorCreateSessionFuncNotRegistered {
	return &errorCreateSessionFuncNotRegistered{name: name}
}

func (e *errorCreateSessionFuncNotRegistered) Error() string {
	return fmt.Sprintf("entry \"%s\" is not defined in create session func catalog", e.name)
}

func init() {
	createSessionCatalogData = make(map[string]func(func() (*gocql.ClusterConfig, error)) (commongocql.GocqlSession, error))
	RegisterCreateSessionFunc("", commongocql.CreateSession)
}

// RegisterCreateSessionFunc needs to be called when using a custom `CreateSessionFunc`
// in the cassandra config. The `name` here must correspond to the `crateSessionFunc`
// member of the YAML config file.
func RegisterCreateSessionFunc(name string, callback func(func() (*gocql.ClusterConfig, error)) (commongocql.GocqlSession, error)) {
	createSessionCatalogMutex.Lock()
	defer createSessionCatalogMutex.Unlock()

	createSessionCatalogData[name] = callback
}

func getCreateSessionFunc(name string) (func(func() (*gocql.ClusterConfig, error)) (commongocql.GocqlSession, error), error) {
	createSessionCatalogMutex.RLock()
	defer createSessionCatalogMutex.RUnlock()

	callback, ok := createSessionCatalogData[name]
	if callback == nil || !ok {
		// If anything is nil, not declared, not found -> fail early.
		return nil, newErrorCreateSessionFuncNoRegistered(name)
	}
	return callback, nil
}
