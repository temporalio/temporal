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
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commongocql "go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

type testErrNotImplemented struct{}

func (e testErrNotImplemented) Error() string {
	return "not implemented, this is a test"
}

func testCustomCreateSession(_clusterFunc func() (*gocql.ClusterConfig, error)) (commongocql.GocqlSession, error) {
	return nil, testErrNotImplemented{}
}

func testClusterFunc() (*gocql.ClusterConfig, error) {
	return gocql.NewCluster("this-is-a-test.this-host-is-not-defined.this-domain-does-not-exist"), nil
}

func TestCreateSessionCatalog(t *testing.T) {

	createSession, err := getCreateSessionFunc("")
	assert.NoError(t, err)
	require.NotNil(t, createSession,
		"default create session func is declared")
	session, err := createSession(testClusterFunc)
	assert.Nil(t, session)
	require.NotNil(t, err)
	assert.Contains(t, err.Error(), "gocql:",
		"default create session func is using upstream implementation")

	createSession, err = getCreateSessionFunc("custom")
	require.NotNil(t, err)
	assert.Contains(t, err.Error(), "custom",
		"custom create session func not defined yet")
	assert.Nil(t, createSession)

	RegisterCreateSessionFunc("custom", testCustomCreateSession)

	createSession, err = getCreateSessionFunc("custom")
	assert.NoError(t, err)
	require.NotNil(t, createSession,
		"custom create session is defined")
	session, err = createSession(testClusterFunc)
	assert.Nil(t, session)
	require.NotNil(t, err)
	assert.Equal(t, "not implemented, this is a test", err.Error(),
		"custom create session func is using custom implementation")

	createSession, err = getCreateSessionFunc("")
	assert.NoError(t, err)
	require.NotNil(t, commongocql.CreateSession, createSession,
		"default create session func is still declaared")
	session, err = createSession(testClusterFunc)
	assert.Nil(t, session)
	require.NotNil(t, err)
	assert.Contains(t, err.Error(), "gocql:",
		"default create session func is still using upstream implementation")

	RegisterCreateSessionFunc("", testCustomCreateSession)

	createSession, err = getCreateSessionFunc("")
	assert.NoError(t, err)
	require.NotNil(t, createSession,
		"custom create session is still defined")
	session, err = createSession(testClusterFunc)
	assert.Nil(t, session)
	require.NotNil(t, err)
	assert.Equal(t, "not implemented, this is a test", err.Error(),
		"custom create session func is still using custom implementation")
}
