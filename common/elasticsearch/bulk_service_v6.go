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

func (b *bulkServiceV6) Do(ctx context.Context) (*elastic.BulkResponse, error) {
	_, err := b.esBulkService.Do(ctx)
	// TODO (alex): BulkResponse is a complex structure and is not used by caller. Implement converter before using it in caller code.
	return nil, err
}

func (b *bulkServiceV6) NumberOfActions() int {
	return b.esBulkService.NumberOfActions()
}

func (b *bulkServiceV6) Add(request elastic.BulkableRequest) {
	b.esBulkService.Add(request)
}
