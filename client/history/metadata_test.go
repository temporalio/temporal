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

package history

import (
	"math/rand"
	"strconv"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/metadata"
)

type (
	metadataSuite struct {
		suite.Suite
		*require.Assertions

		controller *gomock.Controller
	}
)

func TestMetadataSuite(t *testing.T) {
	s := new(metadataSuite)
	suite.Run(t, s)
}

func (s *metadataSuite) SetupSuite() {
}

func (s *metadataSuite) TearDownSuite() {
}

func (s *metadataSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())

}

func (s *metadataSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *metadataSuite) TestClusterShardMD_Encode_Decode() {
	clientClusterShardID := ClusterShardID{
		ClusterID: rand.Int31(),
		ShardID:   rand.Int31(),
	}
	serverClusterShardID := ClusterShardID{
		ClusterID: rand.Int31(),
		ShardID:   rand.Int31(),
	}

	clusterShardMD := EncodeClusterShardMD(
		clientClusterShardID,
		serverClusterShardID,
	)
	actualClientClusterShardID, actualServerClusterShardID, err := DecodeClusterShardMD(clusterShardMD)
	s.NoError(err)
	s.Equal(clientClusterShardID, actualClientClusterShardID)
	s.Equal(serverClusterShardID, actualServerClusterShardID)
}

func (s *metadataSuite) TestClusterShardMD_Decode_Error() {
	clusterShardMD := metadata.Pairs(
		MetadataKeyClientShardID, strconv.Itoa(int(rand.Int31())),
		MetadataKeyServerClusterID, uuid.NewString(),
		MetadataKeyServerShardID, strconv.Itoa(int(rand.Int31())),
	)
	_, _, err := DecodeClusterShardMD(clusterShardMD)
	s.Error(err)

	clusterShardMD = metadata.Pairs(
		MetadataKeyClientClusterID, uuid.NewString(),
		MetadataKeyServerClusterID, uuid.NewString(),
		MetadataKeyServerShardID, strconv.Itoa(int(rand.Int31())),
	)
	_, _, err = DecodeClusterShardMD(clusterShardMD)
	s.Error(err)

	clusterShardMD = metadata.Pairs(
		MetadataKeyClientClusterID, uuid.NewString(),
		MetadataKeyClientShardID, strconv.Itoa(int(rand.Int31())),
		MetadataKeyServerShardID, strconv.Itoa(int(rand.Int31())),
	)
	_, _, err = DecodeClusterShardMD(clusterShardMD)
	s.Error(err)

	clusterShardMD = metadata.Pairs(
		MetadataKeyClientClusterID, uuid.NewString(),
		MetadataKeyClientShardID, strconv.Itoa(int(rand.Int31())),
		MetadataKeyServerClusterID, uuid.NewString(),
	)
	_, _, err = DecodeClusterShardMD(clusterShardMD)
	s.Error(err)

	clusterShardMD = metadata.Pairs(
		MetadataKeyClientClusterID, uuid.NewString(),
		MetadataKeyClientShardID, uuid.NewString(),
		MetadataKeyServerClusterID, uuid.NewString(),
		MetadataKeyServerShardID, uuid.NewString(),
	)
	_, _, err = DecodeClusterShardMD(clusterShardMD)
	s.Error(err)
}
