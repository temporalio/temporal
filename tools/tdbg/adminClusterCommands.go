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

package tdbg

import (
	"fmt"

	"github.com/urfave/cli/v2"

	"go.temporal.io/server/api/adminservice/v1"
)

// AdminDescribeCluster is used to dump information about the cluster
func AdminDescribeCluster(c *cli.Context) error {
	adminClient := cFactory.AdminClient(c)

	ctx, cancel := newContext(c)
	defer cancel()
	clusterName := c.String(FlagCluster)
	response, err := adminClient.DescribeCluster(ctx, &adminservice.DescribeClusterRequest{
		ClusterName: clusterName,
	})
	if err != nil {
		return fmt.Errorf("Operation DescribeCluster failed.: %s", err)
	}

	prettyPrintJSONObject(response)
	return nil
}

// AdminListClusters is used to fetch information about all clusters
func AdminListClusters(c *cli.Context) error {
	adminClient := cFactory.AdminClient(c)
	var token []byte

	pageSize := c.Int(FlagPageSize)
	for more := true; more; more = len(token) > 0 {
		if more && len(token) > 0 {
			if !showNextPage() {
				break
			}
		}
		ctx, cancel := newContext(c)
		response, err := adminClient.ListClusters(ctx, &adminservice.ListClustersRequest{
			PageSize:      int32(pageSize),
			NextPageToken: token,
		})
		cancel()
		if err != nil {
			return fmt.Errorf("Operation ListClusters failed.: %s", err)
		}
		token = response.GetNextPageToken()
		if len(response.GetClusters()) > 0 {
			prettyPrintJSONObject(response.GetClusters())
		}
	}
	return nil
}

// AdminAddOrUpdateRemoteCluster is used to add or update remote cluster information
func AdminAddOrUpdateRemoteCluster(c *cli.Context) error {
	adminClient := cFactory.AdminClient(c)
	ctx, cancel := newContext(c)
	defer cancel()

	_, err := adminClient.AddOrUpdateRemoteCluster(ctx, &adminservice.AddOrUpdateRemoteClusterRequest{
		FrontendAddress:               c.String(FlagFrontendAddress),
		EnableRemoteClusterConnection: c.Bool(FlagConnectionEnable),
	})
	if err != nil {
		return fmt.Errorf("Operation AddOrUpdateRemoteCluster failed.: %s", err)
	}
	return nil
}

// AdminRemoveRemoteCluster is used to remove remote cluster information from the cluster
func AdminRemoveRemoteCluster(c *cli.Context) error {
	adminClient := cFactory.AdminClient(c)

	ctx, cancel := newContext(c)
	defer cancel()
	clusterName := c.String(FlagCluster)
	_, err := adminClient.RemoveRemoteCluster(ctx, &adminservice.RemoveRemoteClusterRequest{
		ClusterName: clusterName,
	})
	if err != nil {
		return fmt.Errorf("Operation RemoveRemoteCluster failed.: %s", err)
	}
	return nil
}
