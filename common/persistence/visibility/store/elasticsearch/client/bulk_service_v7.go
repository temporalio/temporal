package client

import (
	"context"

	"github.com/olivere/elastic/v7"
)

type (
	bulkServiceImpl struct {
		esBulkService *elastic.BulkService
	}
)

func newBulkService(esBulkService *elastic.BulkService) *bulkServiceImpl {
	return &bulkServiceImpl{
		esBulkService: esBulkService,
	}
}

func (b *bulkServiceImpl) Do(ctx context.Context) error {
	_, err := b.esBulkService.Do(ctx)
	return err
}

func (b *bulkServiceImpl) NumberOfActions() int {
	return b.esBulkService.NumberOfActions()
}

func (b *bulkServiceImpl) Add(request *BulkableRequest) {
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
