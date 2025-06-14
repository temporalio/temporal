package testenv

import (
	"cmp"
	"context"
	"sync"

	"github.com/pborman/uuid"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/stamp"
	"go.temporal.io/server/tests/acceptance/model"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

type grpcInterceptor struct {
	lock     sync.RWMutex
	clusters map[stamp.ID]*Cluster
}

func newGrpcInterceptor() *grpcInterceptor {
	return &grpcInterceptor{
		clusters: make(map[stamp.ID]*Cluster),
	}
}

func (i *grpcInterceptor) addCluster(cluster *Cluster) {
	i.lock.Lock()
	defer i.lock.Unlock()

	i.clusters[cluster.GetID()] = cluster
}

func (i *grpcInterceptor) removeCluster(cluster *Cluster) {
	i.lock.Lock()
	defer i.lock.Unlock()

	delete(i.clusters, cluster.GetID())
}

func (i *grpcInterceptor) Interceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		// Extract routing information from request's metadata.
		var clusterID stamp.ID
		var actionID stamp.ActID
		md, ok := metadata.FromIncomingContext(ctx)
		if ok {
			if id, ok := md[clusterIdKey]; ok {
				clusterID = stamp.ID(id[0])
			}
			if id, ok := md[actionIdKey]; ok {
				actionID = stamp.ActID(id[0])
			}
		}
		actionID = cmp.Or(actionID, stamp.ActID(uuid.New()))

		// Short-circuit by forwarding request, if no cluster found.
		if clusterID == "" {
			return handler(ctx, req)
		}

		// Identify the cluster to route the request.
		i.lock.RLock()
		cluster := i.clusters[clusterID]
		i.lock.RUnlock()

		// Route the request.
		var onRespFuncs []stamp.OnComplete
		incAction := model.IncomingAction[proto.Message]{
			ActionID:       actionID,
			Cluster:        clusterID,
			RequestID:      uuid.New(),
			RequestHeaders: md,
			Method:         info.FullMethod,
			Request:        common.CloneProto(req.(proto.Message)),
			ValidationErrs: &[]string{},
		}

		// handle request in the model
		onResp := cluster.mdlEnv.Route(&incAction)
		if onResp != nil {
			onRespFuncs = append(onRespFuncs, onResp)
		}

		// Process the request.
		resp, err := handler(ctx, req)

		// Route the response.
		for _, onResp := range onRespFuncs {
			outAction := model.OutgoingAction[proto.Message]{
				ActID: actionID,
			}
			if err == nil {
				outAction.Response = common.CloneProto(resp.(proto.Message))
			} else {
				outAction.ResponseErr = err
			}
			onResp(outAction)
		}

		return resp, err
	}
}
