package elasticsearch

import (
	"context"

	"github.com/olivere/elastic/v7"
)

type (
	bulkServiceV7 struct {
		esBulkService *elastic.BulkService
	}
)

func newBulkServiceV7(esBulkService *elastic.BulkService) *bulkServiceV7 {
	return &bulkServiceV7{
		esBulkService: esBulkService,
	}
}

func (b *bulkServiceV7) Do(ctx context.Context) (*elastic.BulkResponse, error) {
	return b.esBulkService.Do(ctx)
}

func (b *bulkServiceV7) NumberOfActions() int {
	return b.esBulkService.NumberOfActions()
}

func (b *bulkServiceV7) Add(request *BulkableRequest) {
	switch request.RequestType {
	case BulkableRequestTypeIndex:
		bulkDeleteRequest := elastic.NewBulkIndexRequest().
			Index(request.Index).
			Id(request.ID).
			VersionType(versionTypeExternal).
			Version(request.Version).
			Doc(request.Doc)
		b.esBulkService.Add(bulkDeleteRequest)
	case BulkableRequestTypeDelete:
		bulkDeleteRequest := elastic.NewBulkDeleteRequest().
			Index(request.Index).
			Id(request.ID).
			VersionType(versionTypeExternal).
			Version(request.Version)
		b.esBulkService.Add(bulkDeleteRequest)
	}
}
