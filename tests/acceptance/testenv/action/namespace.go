package action

import (
	"time"

	"github.com/pborman/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/stamp"
	"go.temporal.io/server/tests/acceptance/model"
	"google.golang.org/protobuf/types/known/durationpb"
)

type CreateNamespace struct {
	stamp.ActionActor[*model.Cluster]
	stamp.ActionTarget[*model.Namespace]
	Name               stamp.Gen[stamp.ID]
	ID                 stamp.Gen[stamp.ID]
	NamespaceRetention stamp.Gen[time.Duration]
}

func (c CreateNamespace) Next(ctx stamp.GenContext) *persistence.CreateNamespaceRequest {
	return &persistence.CreateNamespaceRequest{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:          namespace.ID(uuid.New()).String(), // TODO use generator
				Name:        "ns-" + string(c.Name.Next(ctx.AllowRandom())),
				State:       enumspb.NAMESPACE_STATE_REGISTERED,
				Description: "namespace for acceptance tests",
			},
			Config: &persistencespb.NamespaceConfig{
				Retention:   durationpb.New(c.NamespaceRetention.NextOrDefault(ctx, 24*time.Hour)),
				BadBinaries: &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
			},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: string(c.GetActor().GetID()),
				Clusters: []string{
					string(c.GetActor().GetID()),
				},
			},
		},
	}
}

type SetNamespaceConfig[T any] struct {
	stamp.ActionActor[*model.Cluster]
	stamp.ActionTarget[*model.Namespace]
	NS  *model.Namespace                       `validate:"required"`
	Key dynamicconfig.NamespaceTypedSetting[T] `validate:"required"`
	Val T                                      `validate:"required"`
}

func (c SetNamespaceConfig[T]) Next(ctx stamp.GenContext) model.ClusterConfigChanged {
	return model.ClusterConfigChanged{
		Key: c.Key,
		Vals: []dynamicconfig.ConstrainedValue{{
			Value: c.Val,
			Constraints: dynamicconfig.Constraints{
				Namespace: string(c.NS.GetID()),
			},
		}},
	}
}
