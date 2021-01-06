//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination bulk_processor_mock.go

package elasticsearch

import (
	"github.com/olivere/elastic/v7"
)

type (
	BulkProcessor interface {
		Stop() error
		Add(request elastic.BulkableRequest)
	}
)

var _ BulkProcessor = (*elastic.BulkProcessor)(nil)
