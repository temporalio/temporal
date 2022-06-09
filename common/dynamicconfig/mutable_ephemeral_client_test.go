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

package dynamicconfig_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	dconf "go.temporal.io/server/common/dynamicconfig"
)

func TestMutations(t *testing.T) {
	c := dconf.NewMutableEphemeralClient()
	shardInterval := 22122 * time.Second
	maxbad := 22122
	c.Set(dconf.AcquireShardInterval, shardInterval)
	c.Add(dconf.FrontendMaxBadBinaries, maxbad)

	d, err := c.GetDurationValue(dconf.AcquireShardInterval, nil, 1*time.Second)
	require.NoError(t, err)
	require.Equal(t, d, shardInterval)

	i, err := c.GetIntValue(dconf.FrontendMaxBadBinaries, nil, 100)
	require.NoError(t, err)
	require.Equal(t, i, maxbad)

	b, err := c.GetBoolValue(dconf.EnableAuthorization, nil, true)
	require.Error(t, err)
	require.True(t, b)
}

func TestConstrainedMutation(t *testing.T) {
	c := dconf.NewMutableEphemeralClient()
	maxbad := 22122
	c.Add(dconf.FrontendMaxBadBinaries, maxbad, dconf.ForNamespace("nsfoo"))

	i, err := c.GetIntValue(dconf.FrontendMaxBadBinaries, nil, 100)
	require.Error(t, err, "expected error from lookup with no namespace")
	require.Equal(t, i, 100)

	i, err = c.GetIntValue(dconf.FrontendMaxBadBinaries,
		composeFilters(dconf.NamespaceFilter("wrong_namespace")), 100)
	require.Error(t, err)
	require.Equal(t, i, 100)

	i, err = c.GetIntValue(dconf.FrontendMaxBadBinaries,
		composeFilters(dconf.NamespaceFilter("nsfoo")), 100)
	require.NoError(t, err)
	require.Equal(t, i, maxbad)
}

func TestMulti(t *testing.T) {
	c := dconf.NewMutableEphemeralClient()
	shardInterval := 22122 * time.Second
	maxbad := 22122
	c.MSet(map[dconf.Key]interface{}{
		dconf.AcquireShardInterval:   shardInterval,
		dconf.FrontendMaxBadBinaries: maxbad,
	})
	c.MAdd(map[dconf.Key]interface{}{
		dconf.AcquireShardInterval:   1 * time.Second,
		dconf.FrontendMaxBadBinaries: 1,
	}, dconf.ForNamespace("nsfoo"))

	d, err := c.GetDurationValue(dconf.AcquireShardInterval, nil, shardInterval)
	require.NoError(t, err)
	require.Equal(t, d, shardInterval)

	i, err := c.GetIntValue(dconf.FrontendMaxBadBinaries, nil, maxbad)
	require.NoError(t, err)
	require.Equal(t, i, maxbad)

	nsflt := composeFilters(dconf.NamespaceFilter("nsfoo"))
	d, err = c.GetDurationValue(dconf.AcquireShardInterval, nsflt, 1*time.Hour)
	require.NoError(t, err)
	require.Equal(t, d, 1*time.Second)

	i, err = c.GetIntValue(dconf.FrontendMaxBadBinaries, nsflt, 0)
	require.NoError(t, err)
	require.Equal(t, i, 1)
}

func composeFilters(fs ...dconf.FilterOption) []map[dconf.Filter]interface{} {
	out := map[dconf.Filter]interface{}{}
	for _, f := range fs {
		f(out)
	}
	return []map[dconf.Filter]interface{}{out}
}
