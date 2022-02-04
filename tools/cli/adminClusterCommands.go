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

package cli

import (
	"fmt"

	"github.com/urfave/cli"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/cassandra"

	"go.temporal.io/server/api/adminservice/v1"
)

// AdminDescribeCluster is used to dump information about the cluster
func AdminDescribeCluster(c *cli.Context) {
	adminClient := cFactory.AdminClient(c)

	ctx, cancel := newContext(c)
	defer cancel()
	clusterName := c.String(FlagCluster)
	response, err := adminClient.DescribeCluster(ctx, &adminservice.DescribeClusterRequest{
		ClusterName: clusterName,
	})
	if err != nil {
		ErrorAndExit("Operation DescribeCluster failed.", err)
	}

	prettyPrintJSONObject(response)
}

// AdminAddOrUpdateRemoteCluster is used to add or update remote cluster information
func AdminAddOrUpdateRemoteCluster(c *cli.Context) {
	adminClient := cFactory.AdminClient(c)
	ctx, cancel := newContext(c)
	defer cancel()

	_, err := adminClient.AddOrUpdateRemoteCluster(ctx, &adminservice.AddOrUpdateRemoteClusterRequest{
		FrontendAddress:               getRequiredOption(c, FlagFrontendAddress),
		EnableRemoteClusterConnection: c.BoolT(FlagConnectionEnable),
	})
	if err != nil {
		ErrorAndExit("Operation AddOrUpdateRemoteCluster failed.", err)
	}
}

// AdminRemoveRemoteCluster is used to remove remote cluster information from the cluster
func AdminRemoveRemoteCluster(c *cli.Context) {
	adminClient := cFactory.AdminClient(c)

	ctx, cancel := newContext(c)
	defer cancel()
	clusterName := getRequiredOption(c, FlagCluster)
	_, err := adminClient.RemoveRemoteCluster(ctx, &adminservice.RemoveRemoteClusterRequest{
		ClusterName: clusterName,
	})
	if err != nil {
		ErrorAndExit("Operation RemoveRemoteCluster failed.", err)
	}
}

func AdminUpdateClusterName(c *cli.Context) {
	currentCluster := c.String(FlagCluster)
	newCluster := c.String(FlagNewCluster)

	session := connectToCassandra(c)
	clusterStore, err := cassandra.NewClusterMetadataStore(session, log.NewNoopLogger())
	if err != nil {
		ErrorAndExit("Failed to connect to Cassandra", err)
	}
	clusterMetadataManager := persistence.NewClusterMetadataManagerImpl(clusterStore, currentCluster, log.NewNoopLogger())

	currentClusterMetadata, err := clusterMetadataManager.GetClusterMetadata(&persistence.GetClusterMetadataRequest{ClusterName: currentCluster})
	if err != nil {
		ErrorAndExit("Failed to get current cluster metadata", err)
	}

	applied, err := clusterMetadataManager.SaveClusterMetadata(&persistence.SaveClusterMetadataRequest{
		ClusterMetadata: persistencespb.ClusterMetadata{
			ClusterName:              newCluster,
			HistoryShardCount:        currentClusterMetadata.HistoryShardCount,
			ClusterId:                currentClusterMetadata.ClusterId,
			VersionInfo:              currentClusterMetadata.VersionInfo,
			IndexSearchAttributes:    currentClusterMetadata.IndexSearchAttributes,
			ClusterAddress:           currentClusterMetadata.ClusterAddress,
			FailoverVersionIncrement: currentClusterMetadata.FailoverVersionIncrement,
			InitialFailoverVersion:   currentClusterMetadata.InitialFailoverVersion,
			IsGlobalNamespaceEnabled: currentClusterMetadata.IsGlobalNamespaceEnabled,
			IsConnectionEnabled:      currentClusterMetadata.IsConnectionEnabled,
		},
		Version: 0,
	})
	if !applied || err != nil {
		ErrorAndExit("Failed to create new cluster metadata", err)
	}
	// Use raw store client to delete
	err = clusterStore.DeleteClusterMetadata(&persistence.InternalDeleteClusterMetadataRequest{ClusterName: currentCluster})
	if err != nil {
		ErrorAndExit("Failed to delete old cluster metadata", err)
	}
	fmt.Println("Successfully updated cluster name from ", currentCluster, " to ", newCluster)
}

func AdminBackfillNamespaceWithClusterName(c *cli.Context) {
	newCluster := c.String(FlagNewCluster)

	session := connectToCassandra(c)
	metadataStore, err := cassandra.NewMetadataStore(newCluster, session, log.NewNoopLogger())
	if err != nil {
		ErrorAndExit("Failed to connect to Cassandra", err)
	}
	metadataManager := persistence.NewMetadataManagerImpl(metadataStore, log.NewNoopLogger(), newCluster)

	var backfillNamepsaceList []*persistencespb.NamespaceDetail
	var nextPageToken []byte
	for {
		listResp, err := metadataManager.ListNamespaces(&persistence.ListNamespacesRequest{PageSize: 1000, NextPageToken: nextPageToken})
		if err != nil {
			ErrorAndExit("Failed to list all namespaces", err)
		}
		for _, resp := range listResp.Namespaces {
			if resp.IsGlobalNamespace {
				ErrorAndExit(fmt.Sprintf("Validation failed. Found global namespace: %s", resp.Namespace.String()), err)
			}
			if len(resp.Namespace.ReplicationConfig.Clusters) > 1 {
				ErrorAndExit(fmt.Sprintf("Validation failed. Found non single cluster namespace: %s", resp.Namespace.String()), err)
			}
			backfillNamepsaceList = append(backfillNamepsaceList, resp.Namespace)
		}

		if len(listResp.NextPageToken) == 0 {
			break
		}
		nextPageToken = listResp.NextPageToken
	}

	for _, namespace := range backfillNamepsaceList {
		metadata, err := metadataManager.GetMetadata()
		if err != nil {
			ErrorAndExit("Failed to get namespace metadata", err)
		}

		err = metadataManager.UpdateNamespace(&persistence.UpdateNamespaceRequest{
			Namespace: &persistencespb.NamespaceDetail{
				Info:   namespace.Info,
				Config: namespace.Config,
				ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
					ActiveClusterName: newCluster,
					Clusters:          []string{newCluster},
					State:             namespace.ReplicationConfig.State,
				},
				ConfigVersion:               namespace.ConfigVersion + 1,
				FailoverNotificationVersion: namespace.FailoverNotificationVersion,
				FailoverVersion:             namespace.FailoverVersion,
				FailoverEndTime:             namespace.FailoverEndTime,
			},
			IsGlobalNamespace:   false,
			NotificationVersion: metadata.NotificationVersion,
		})
		if err != nil {
			ErrorAndExit(fmt.Sprintf("Failed to update namespace: %s", namespace.Info.Name), err)
		}
	}
	fmt.Println("Successfully backfill all namespace cluster to: ", newCluster)
}
