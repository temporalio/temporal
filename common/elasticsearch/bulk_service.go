package elasticsearch

import (
	"context"

	"github.com/olivere/elastic/v7"
)

type (
	BulkService interface {
		Do(ctx context.Context) (*elastic.BulkResponse, error)
		NumberOfActions() int
		Add(request *BulkableRequest)
	}
)
