package historydlq

import (
	"go.temporal.io/server/chasm"
	historydlqpb "go.temporal.io/server/chasm/lib/historydlq/gen/historydlqpb/v1"
	"google.golang.org/grpc"
)

const (
	libraryName          = "historydlq"
	segmentComponentName = "segment"
	entryComponentName   = "entry"
)

type Library struct {
	chasm.UnimplementedLibrary
	handler *serviceHandler
}

func NewNilLibrary() *Library { return &Library{} }

func newLibrary(handler *serviceHandler) *Library { return &Library{handler: handler} }

func (l *Library) RegisterServices(server *grpc.Server) {
	if l.handler != nil {
		historydlqpb.RegisterHistoryDLQServiceServer(server, l.handler)
	}
}

func (l *Library) Name() string { return libraryName }

func (l *Library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Segment](segmentComponentName,
			chasm.WithBusinessIDAlias("HistoryDLQSegmentBusinessId"),
			chasm.WithSearchAttributes(categorySearchAttribute, sourceSearchAttribute, minMessageIDSearchAttribute)),
		chasm.NewRegistrableComponent[*Entry](entryComponentName),
	}
}

var (
	categorySearchAttribute     = chasm.NewSearchAttributeInt("HistoryDLQCategory", chasm.SearchAttributeFieldInt01)
	sourceSearchAttribute       = chasm.NewSearchAttributeKeyword("HistoryDLQSourceCluster", chasm.SearchAttributeFieldKeyword01)
	minMessageIDSearchAttribute = chasm.NewSearchAttributeInt("HistoryDLQMinMessageID", chasm.SearchAttributeFieldInt02)
)
