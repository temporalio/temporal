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

func (b *bulkServiceV7) Add(request elastic.BulkableRequest) {
	b.esBulkService.Add(request)
}
