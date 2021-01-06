package elasticsearch

import (
	"github.com/olivere/elastic/v7"
)

type (
	bulkProcessorV7 struct {
		esBulkProcessor *elastic.BulkProcessor
	}
)

func newBulkProcessorV7(esBulkProcessor *elastic.BulkProcessor) *bulkProcessorV7 {
	if esBulkProcessor == nil {
		return nil
	}
	return &bulkProcessorV7{
		esBulkProcessor: esBulkProcessor,
	}
}

func (p *bulkProcessorV7) Stop() error {
	return p.esBulkProcessor.Stop()
}

func (p *bulkProcessorV7) Add(request elastic.BulkableRequest) {
	p.esBulkProcessor.Add(request)
}
