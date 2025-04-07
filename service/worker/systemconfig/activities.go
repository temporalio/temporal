package systemconfig

import (
	"context"
	"errors"
	"fmt"

	"go.temporal.io/server/common/persistence"
)

type (
	UpdateFailoverVersionIncrementInput struct {
		CurrentFVI int64
		NewFVI     int64
	}

	UpdateFailoverVersionIncrementOutput struct {
	}

	activities struct {
		currentClusterName     string
		clusterMetadataManager persistence.ClusterMetadataManager
	}
)

func (a *activities) UpdateFVI(
	ctx context.Context,
	input UpdateFailoverVersionIncrementInput,
) (UpdateFailoverVersionIncrementOutput, error) {
	request := &persistence.GetClusterMetadataRequest{
		ClusterName: a.currentClusterName,
	}
	if input.NewFVI <= 0 {
		return UpdateFailoverVersionIncrementOutput{},
			errors.New("invalid failover version increment")
	}
	resp, err := a.clusterMetadataManager.GetClusterMetadata(ctx, request)
	if err != nil {
		return UpdateFailoverVersionIncrementOutput{}, err
	}
	metadata := resp.ClusterMetadata
	if metadata.FailoverVersionIncrement != input.CurrentFVI {
		return UpdateFailoverVersionIncrementOutput{},
			errors.New(fmt.Sprintf("failover version increment %v does not match input version %v", metadata.FailoverVersionIncrement, input.CurrentFVI))
	}
	if metadata.InitialFailoverVersion == input.NewFVI {
		return UpdateFailoverVersionIncrementOutput{}, nil
	}
	if metadata.InitialFailoverVersion > input.NewFVI {
		return UpdateFailoverVersionIncrementOutput{},
			errors.New(fmt.Sprintf("failover version increment %v is less than initial failover version %v", input.NewFVI, metadata.InitialFailoverVersion))
	}
	if !metadata.IsGlobalNamespaceEnabled {
		return UpdateFailoverVersionIncrementOutput{},
			errors.New("please update failover version increment from application yaml file")
	}

	metadata.FailoverVersionIncrement = input.NewFVI
	applied, err := a.clusterMetadataManager.SaveClusterMetadata(ctx, &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: metadata,
		Version:         resp.Version,
	})
	if err != nil {
		return UpdateFailoverVersionIncrementOutput{}, err
	}
	if !applied {
		return UpdateFailoverVersionIncrementOutput{}, errors.New("new failover version increment did not apply")
	}
	return UpdateFailoverVersionIncrementOutput{}, nil
}
