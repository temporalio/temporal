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
	"os"

	"github.com/urfave/cli/v2"

	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
)

const (
	defaultPageSize = 1000
)

type (
	DLQService interface {
		ReadMessages(c *cli.Context) error
		PurgeMessages(c *cli.Context) error
		MergeMessages(c *cli.Context) error
	}
)

// GetDLQService returns a DLQService based on FlagDLQVersion.
func GetDLQService(c *cli.Context, clientFactory ClientFactory) (DLQService, error) {
	version := c.String(FlagDLQVersion)
	if version == "v1" {
		return NewDLQV1Service(clientFactory), nil
	}
	if version == "v2" {
		return getDLQV2Service(c, clientFactory)
	}
	return nil, fmt.Errorf("unknown DLQ version: %v", version)
}

func getDLQV2Service(c *cli.Context, clientFactory ClientFactory) (DLQService, error) {
	dlqType := c.String(FlagDLQType)
	category, ok, err := getCategoryByID(dlqType)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("unknown dlq category %v", dlqType)
	}
	sourceCluster := c.String(FlagCluster)
	if len(sourceCluster) == 0 {
		return nil, fmt.Errorf("--%s is not provided", FlagCluster)
	}
	targetCluster, service, err := getTargetCluster(c, clientFactory)
	if err != nil {
		return service, err
	}
	return NewDLQV2Service(category, sourceCluster, targetCluster, clientFactory), nil
}

func getTargetCluster(c *cli.Context, clientFactory ClientFactory) (string, DLQService, error) {
	targetCluster := c.String(FlagTargetCluster)
	if len(targetCluster) == 0 {
		client := clientFactory.AdminClient(c)
		cluster, err := client.DescribeCluster(c.Context, &adminservice.DescribeClusterRequest{})
		if err != nil {
			return "", nil, fmt.Errorf(
				"can't figure out current cluster name to set default value of --%s because DescribeCluster failed: %v",
				FlagTargetCluster,
				err,
			)
		}
		targetCluster = cluster.ClusterName
	}
	return targetCluster, nil, nil
}

func toQueueType(dlqType string) (enumsspb.DeadLetterQueueType, error) {
	switch dlqType {
	case "namespace":
		return enumsspb.DEAD_LETTER_QUEUE_TYPE_NAMESPACE, nil
	case "history":
		return enumsspb.DEAD_LETTER_QUEUE_TYPE_REPLICATION, nil
	default:
		return enumsspb.DEAD_LETTER_QUEUE_TYPE_UNSPECIFIED, fmt.Errorf("unsupported Tueue type %v", dlqType)
	}
}

func getOutputFile(outputFile string) (*os.File, error) {
	if len(outputFile) == 0 {
		return os.Stdout, nil
	}
	f, err := os.Create(outputFile)
	if err != nil {
		return nil, fmt.Errorf("failed to create output file: %s", err)
	}
	return f, nil
}
