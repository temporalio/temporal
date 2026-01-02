// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
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

package client

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
)

// TopologyType represents the MongoDB deployment topology
type TopologyType int

const (
	// TopologyUnknown indicates topology could not be determined
	TopologyUnknown TopologyType = iota
	// TopologyStandalone indicates a single MongoDB instance
	TopologyStandalone
	// TopologyReplicaSet indicates a replica set deployment
	TopologyReplicaSet
	// TopologySharded indicates a sharded cluster deployment
	TopologySharded
)

// String returns the string representation of the topology type
func (t TopologyType) String() string {
	switch t {
	case TopologyStandalone:
		return "standalone"
	case TopologyReplicaSet:
		return "replicaset"
	case TopologySharded:
		return "sharded"
	default:
		return "unknown"
	}
}

// SupportsTransactions returns true if the topology supports transactions
func (t TopologyType) SupportsTransactions() bool {
	return t == TopologyReplicaSet || t == TopologySharded
}

// TopologyInfo contains information about the MongoDB deployment topology
type TopologyInfo struct {
	Type           TopologyType
	ReplicaSetName string
	ShardCount     int
}

// DetectTopology detects the MongoDB deployment topology
func DetectTopology(ctx context.Context, client Client) (*TopologyInfo, error) {
	db := client.Database("admin")

	// Run isMaster command to get topology information
	var isMasterResult bson.M
	if err := db.RunCommand(ctx, bson.D{{Key: "isMaster", Value: 1}}).Decode(&isMasterResult); err != nil {
		return nil, fmt.Errorf("failed to run isMaster command: %w", err)
	}

	info := &TopologyInfo{
		Type: TopologyUnknown,
	}

	// Check for sharded cluster
	if msg, ok := isMasterResult["msg"].(string); ok && msg == "isdbgrid" {
		info.Type = TopologySharded

		// Get shard count
		var shardResult struct {
			Shards []bson.M `bson:"shards"`
		}
		configDB := client.Database("config")
		if err := configDB.RunCommand(ctx, bson.D{{Key: "listShards", Value: 1}}).Decode(&shardResult); err == nil {
			info.ShardCount = len(shardResult.Shards)
		}

		return info, nil
	}

	// Check for replica set
	if setName, ok := isMasterResult["setName"].(string); ok && setName != "" {
		info.Type = TopologyReplicaSet
		info.ReplicaSetName = setName
		return info, nil
	}

	// Check if it's explicitly standalone
	if _, hasSetName := isMasterResult["setName"]; !hasSetName {
		if _, hasHosts := isMasterResult["hosts"]; !hasHosts {
			info.Type = TopologyStandalone
			return info, nil
		}
	}

	return info, nil
}

// GetTopology is a convenience method on mongoClient to detect topology
func (c *mongoClient) GetTopology(ctx context.Context) (*TopologyInfo, error) {
	return DetectTopology(ctx, c)
}
