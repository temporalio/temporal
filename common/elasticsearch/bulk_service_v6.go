package elasticsearch

import (
	"context"

	elastic6 "github.com/olivere/elastic"
	"github.com/olivere/elastic/v7"
)

type (
	bulkServiceV6 struct {
		esBulkService *elastic6.BulkService
	}
)

func newBulkServiceV6(esBulkService *elastic6.BulkService) *bulkServiceV6 {
	return &bulkServiceV6{
		esBulkService: esBulkService,
	}
}

func (b *bulkServiceV6) Do(ctx context.Context) error {
	_, err := b.esBulkService.Do(ctx)
	return err
}

func (b *bulkServiceV6) NumberOfActions() int {
	return b.esBulkService.NumberOfActions()
}

func (b *bulkServiceV6) Add(request *BulkableRequest) {
	switch request.RequestType {
	case BulkableRequestTypeIndex:
		bulkDeleteRequest := elastic.NewBulkIndexRequest().
			Index(request.Index).
			Type(docTypeV6).
			Id(request.ID).
			VersionType(versionTypeExternal).
			Version(request.Version).
			Doc(request.Doc)
		b.esBulkService.Add(bulkDeleteRequest)
	case BulkableRequestTypeDelete:
		bulkDeleteRequest := elastic.NewBulkDeleteRequest().
			Index(request.Index).
			Type(docTypeV6).
			Id(request.ID).
			VersionType(versionTypeExternal).
			Version(request.Version)
		b.esBulkService.Add(bulkDeleteRequest)
	}
}
