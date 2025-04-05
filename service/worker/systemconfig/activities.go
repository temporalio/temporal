package systemconfig

import (
	"context"
	"fmt"

	"go.temporal.io/server/common/persistence"
)

type (
	UpdateFailoverVersionIncrementInput struct {
		CurrentFVI int64
		NewFVI     int64
	}

	UpdateFailoverVersionIncrementOuput struct {
		CurrentFVI int64
		NewFVI     int64
	}

	activities struct {
		currentClusterName     string
		clusterMetadataManager persistence.ClusterMetadataManager
	}
)

func (a *activities) UpdateFVI(
	ctx context.Context,
	input UpdateFailoverVersionIncrementInput,
) (UpdateFailoverVersionIncrementOuput, error) {
	request := &persistence.GetClusterMetadataRequest{
		ClusterName: a.currentClusterName,
	}
	resp, err := a.clusterMetadataManager.GetClusterMetadata(ctx, request)
	if err != nil {
		return UpdateFailoverVersionIncrementOuput{}, err
	}
	metadata := resp.ClusterMetadata
	if metadata.FailoverVersionIncrement != input.CurrentFVI {
		return UpdateFailoverVersionIncrementOuput{},
			fmt.Errorf("failover version increment %v does not match input version %v", metadata.FailoverVersionIncrement, input.CurrentFVI)
	}
	if metadata.InitialFailoverVersion >= input.NewFVI {
		return UpdateFailoverVersionIncrementOuput{},
			fmt.Errorf("failover version increment %v is less than initial failover version %v", input.NewFVI, metadata.InitialFailoverVersion)
	}
	if !metadata.IsGlobalNamespaceEnabled {
		return UpdateFailoverVersionIncrementOuput{},
			fmt.Errorf("please update failover version increment from application yaml file")
	}

	metadata.FailoverVersionIncrement = input.NewFVI
	applied, err := a.clusterMetadataManager.SaveClusterMetadata(ctx, &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: metadata,
		Version:         resp.Version,
	})
	if err != nil {
		return UpdateFailoverVersionIncrementOuput{}, err
	}
	if !applied {
		return UpdateFailoverVersionIncrementOuput{}, fmt.Errorf("new failover version increment did not apply")
	}
	return UpdateFailoverVersionIncrementOuput{}, nil
}
