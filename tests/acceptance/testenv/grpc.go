package testenv

import (
	"cmp"
	"context"
	"sync"

	"github.com/pborman/uuid"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/stamp"
	"go.temporal.io/server/tests/acceptance/model"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

type grpcInterceptor struct {
	mu       sync.RWMutex
	clusters map[*Cluster]struct{}
}

func newGrpcInterceptor(logger log.Logger) *grpcInterceptor {
	return &grpcInterceptor{
		clusters: make(map[*Cluster]struct{}),
	}
}

func (i *grpcInterceptor) addCluster(cluster *Cluster) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.clusters[cluster] = struct{}{}
}

func (i *grpcInterceptor) removeCluster(cluster *Cluster) {
	i.mu.Lock()
	defer i.mu.Unlock()
	delete(i.clusters, cluster)
}

func (i *grpcInterceptor) Interceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		// extract action ID (if any)
		var actID stamp.ActID
		md, ok := metadata.FromIncomingContext(ctx)
		if ok {
			if _, ok := md[actionIdKey]; ok {
				actID = stamp.ActID(md.Get(actionIdKey)[0])
			}
		}
		actID = cmp.Or(actID, stamp.ActID(uuid.New()))

		// get a snapshot of the current clusters to avoid holding the lock during processing
		i.mu.RLock()
		clusters := make([]*Cluster, 0, len(i.clusters))
		for c := range i.clusters {
			clusters = append(clusters, c)
		}
		i.mu.RUnlock()

		// route request
		var onRespFuncs []stamp.OnComplete
		for _, c := range clusters {
			incAction := model.IncomingAction[proto.Message]{
				ActionID:       actID,
				Cluster:        model.ClusterName(c.GetID()),
				RequestID:      uuid.New(),
				RequestHeaders: md,
				Method:         info.FullMethod,
				Request:        common.CloneProto(req.(proto.Message)),
				ValidationErrs: &[]string{},
			}

			// handle request in the model
			onResp := c.mdlEnv.Route(&incAction)
			if onResp != nil {
				onRespFuncs = append(onRespFuncs, onResp)
			}
		}

		// process the request
		resp, err := handler(ctx, req)

		// route response
		for _, onResp := range onRespFuncs {
			outAction := model.OutgoingAction[proto.Message]{
				ActID: actID,
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
