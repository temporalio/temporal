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

package shard

import (
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	contextUtilSuite struct {
		suite.Suite
		*require.Assertions
	}
)

func TestContextUtilSuite(t *testing.T) {
	s := &contextUtilSuite{}
	suite.Run(t, s)
}

func (s *contextUtilSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *contextUtilSuite) TeardownTest() {
	s.Assertions = require.New(s.T())
}

func (s *contextUtilSuite) TestReplicationReaderIDConversion() {
	expectedClusterID := int64(rand.Int31())
	expectedShardID := rand.Int31()

	actualClusterID, actualShardID := ReplicationReaderIDToClusterShardID(
		ReplicationReaderIDFromClusterShardID(expectedClusterID, expectedShardID),
	)
	s.Equal(expectedClusterID, actualClusterID)
	s.Equal(expectedShardID, actualShardID)
}

func (s *contextUtilSuite) TestReplicationReaderIDConversion_1() {
	expectedClusterID := int64(1)
	expectedShardID := int32(1)

	actualClusterID, actualShardID := ReplicationReaderIDToClusterShardID(
		ReplicationReaderIDFromClusterShardID(expectedClusterID, expectedShardID),
	)
	s.Equal(expectedClusterID, actualClusterID)
	s.Equal(expectedShardID, actualShardID)
}

func (s *contextUtilSuite) TestReplicationReaderIDConversion_Int32Max() {
	expectedClusterID := int64(math.MaxInt32)
	expectedShardID := int32(math.MaxInt32)

	actualClusterID, actualShardID := ReplicationReaderIDToClusterShardID(
		ReplicationReaderIDFromClusterShardID(expectedClusterID, expectedShardID),
	)
	s.Equal(expectedClusterID, actualClusterID)
	s.Equal(expectedShardID, actualShardID)
}
