package client

import (
	"context"
)

type (
	BulkService interface {
		Do(ctx context.Context) error
		NumberOfActions() int
		Add(request *BulkableRequest)
	}
)
