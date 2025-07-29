package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

type (
	// bulkProcessorV8 implements BulkProcessor using the official Elasticsearch Go client v8
	bulkProcessorV8 struct {
		esClient   *elasticsearch.Client
		parameters *BulkProcessorParameters
		logger     log.Logger
		requests   []*BulkableRequest
		mutex      sync.Mutex
		stopCh     chan struct{}
		stopped    bool
	}
)

var _ BulkProcessor = (*bulkProcessorV8)(nil)

func newBulkProcessorV8(esClient *elasticsearch.Client, p *BulkProcessorParameters, logger log.Logger) (*bulkProcessorV8, error) {
	processor := &bulkProcessorV8{
		esClient:   esClient,
		parameters: p,
		logger:     logger,
		requests:   make([]*BulkableRequest, 0),
		stopCh:     make(chan struct{}),
	}

	// Start the bulk processor goroutine
	go processor.run()

	return processor, nil
}

func (bp *bulkProcessorV8) run() {
	flushTicker := time.NewTicker(bp.parameters.FlushInterval)
	defer flushTicker.Stop()

	for {
		select {
		case <-bp.stopCh:
			bp.flush()
			return
		case <-flushTicker.C:
			bp.flush()
		}
	}
}

func (bp *bulkProcessorV8) Add(request *BulkableRequest) {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()

	if bp.stopped {
		return
	}

	bp.requests = append(bp.requests, request)

	// Flush if we've reached the bulk size threshold
	if len(bp.requests) >= bp.parameters.BulkSize {
		go bp.flush()
	}
}

func (bp *bulkProcessorV8) flush() {
	bp.mutex.Lock()
	requests := bp.requests
	bp.requests = make([]*BulkableRequest, 0)
	bp.mutex.Unlock()

	if len(requests) == 0 {
		return
	}

	err := bp.executeBulk(requests)
	if err != nil {
		bp.logger.Error("Bulk processor execution failed", tag.Error(err))
		if bp.parameters.AfterFunc != nil {
			bp.parameters.AfterFunc(-1, requests, nil, err)
		}
	}
}

func (bp *bulkProcessorV8) executeBulk(requests []*BulkableRequest) error {
	var bulkBody bytes.Buffer

	for _, req := range requests {
		switch req.RequestType {
		case BulkableRequestTypeIndex:
			// Index action
			action := map[string]interface{}{
				"index": map[string]interface{}{
					"_index": req.Index,
					"_id":    req.ID,
				},
			}
			if req.Version > 0 {
				action["index"].(map[string]interface{})["version"] = req.Version
				action["index"].(map[string]interface{})["version_type"] = "external"
			}

			actionJSON, err := json.Marshal(action)
			if err != nil {
				return err
			}
			bulkBody.Write(actionJSON)
			bulkBody.WriteByte('\n')

			sourceJSON, err := json.Marshal(req.Document)
			if err != nil {
				return err
			}
			bulkBody.Write(sourceJSON)
			bulkBody.WriteByte('\n')

		case BulkableRequestTypeDelete:
			// Delete action
			action := map[string]interface{}{
				"delete": map[string]interface{}{
					"_index": req.Index,
					"_id":    req.ID,
				},
			}
			if req.Version > 0 {
				action["delete"].(map[string]interface{})["version"] = req.Version
				action["delete"].(map[string]interface{})["version_type"] = "external"
			}

			actionJSON, err := json.Marshal(action)
			if err != nil {
				return err
			}
			bulkBody.Write(actionJSON)
			bulkBody.WriteByte('\n')

		default:
			return fmt.Errorf("unsupported bulk request type: %v", req.RequestType)
		}
	}

	// Execute bulk request
	res, err := bp.esClient.Bulk(
		bytes.NewReader(bulkBody.Bytes()),
		bp.esClient.Bulk.WithContext(context.TODO()),
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		return fmt.Errorf("bulk request failed: %s", string(body))
	}

	// Parse response
	respBody, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}

	var bulkResp BulkResponse
	if err := json.Unmarshal(respBody, &bulkResp); err != nil {
		return err
	}

	// Call after function if provided
	if bp.parameters.AfterFunc != nil {
		executionID := int64(time.Now().UnixNano()) // Simple execution ID
		bp.parameters.AfterFunc(executionID, requests, &bulkResp, nil)
	}

	return nil
}

func (bp *bulkProcessorV8) Stop() error {
	bp.mutex.Lock()
	bp.stopped = true
	bp.mutex.Unlock()

	close(bp.stopCh)
	return nil
}
