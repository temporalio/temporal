package client

import (
	"errors"
	"time"

	"github.com/olivere/elastic/v7"
)

type (
	bulkProcessorImpl struct {
		esBulkProcessor *elastic.BulkProcessor
	}
)

func newBulkProcessor(esBulkProcessor *elastic.BulkProcessor) *bulkProcessorImpl {
	if esBulkProcessor == nil {
		return nil
	}
	return &bulkProcessorImpl{
		esBulkProcessor: esBulkProcessor,
	}
}

func (p *bulkProcessorImpl) Stop() error {
	// Flush can block indefinitely if we can't reach ES. Call it with a timeout to avoid
	// blocking server shutdown. Default fx app shutdown timeout is 15s, so use 5s.
	errC := make(chan error)
	go func() {
		errF := p.esBulkProcessor.Flush()
		errS := p.esBulkProcessor.Stop()
		if errF != nil {
			errC <- errF
		} else {
			errC <- errS
		}
	}()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case err := <-errC:
		return err
	case <-timer.C:
		return errors.New("esBulkProcessor Flush/Stop timed out")
	}
}

func (p *bulkProcessorImpl) Add(request *BulkableRequest) {
	switch request.RequestType {
	case BulkableRequestTypeIndex:
		bulkIndexRequest := elastic.NewBulkIndexRequest().
			Index(request.Index).
			Id(request.ID).
			VersionType(versionTypeExternal).
			Version(request.Version).
			Doc(request.Doc)
		p.esBulkProcessor.Add(bulkIndexRequest)
	case BulkableRequestTypeDelete:
		bulkDeleteRequest := elastic.NewBulkDeleteRequest().
			Index(request.Index).
			Id(request.ID).
			VersionType(versionTypeExternal).
			Version(request.Version)
		p.esBulkProcessor.Add(bulkDeleteRequest)
	}
}
