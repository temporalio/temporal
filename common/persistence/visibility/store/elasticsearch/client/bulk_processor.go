package client

import (
	"encoding/json"
	"fmt"
	"time"
)

type BulkableRequestType uint8

const (
	BulkableRequestTypeIndex BulkableRequestType = iota
	BulkableRequestTypeDelete
)

type (
	BulkProcessor interface {
		Stop() error
		Add(request *BulkableRequest)
	}

	// BulkBeforeFunc is called before bulk execution
	BulkBeforeFunc func(executionId int64, requests []*BulkableRequest)

	// BulkAfterFunc is called after bulk execution
	BulkAfterFunc func(executionId int64, requests []*BulkableRequest, response *BulkResponse, err error)

	// BulkProcessorParameters holds all required and optional parameters for executing bulk service
	BulkProcessorParameters struct {
		Name          string
		NumOfWorkers  int
		BulkActions   int
		BulkSize      int
		FlushInterval time.Duration
		BeforeFunc    BulkBeforeFunc
		AfterFunc     BulkAfterFunc
	}

	BulkableRequest struct {
		RequestType BulkableRequestType
		Index       string
		ID          string
		Version     int64
		Document    map[string]interface{}
	}

	// BulkResponse represents a bulk operation response
	BulkResponse struct {
		Took   int                      `json:"took"`
		Errors bool                     `json:"errors"`
		Items  []map[string]interface{} `json:"items"`
	}

	// BulkResponseItem represents a single item in a bulk response
	BulkResponseItem struct {
		Index   string        `json:"_index,omitempty"`
		Type    string        `json:"_type,omitempty"`
		ID      string        `json:"_id,omitempty"`
		Version int64         `json:"_version,omitempty"`
		Status  int           `json:"status,omitempty"`
		Error   *ErrorDetails `json:"error,omitempty"`
	}

	// ErrorDetails represents error details in bulk response
	ErrorDetails struct {
		Type   string `json:"type,omitempty"`
		Reason string `json:"reason,omitempty"`
	}
)

// String returns a string representation of the BulkableRequest
func (br *BulkableRequest) String() string {
	return fmt.Sprintf("BulkableRequest{Type: %d, Index: %s, ID: %s, Version: %d}",
		br.RequestType, br.Index, br.ID, br.Version)
}

// NewBulkIndexRequest creates a new bulk index request
func NewBulkIndexRequest(index, id string) *BulkableRequest {
	return &BulkableRequest{
		RequestType: BulkableRequestTypeIndex,
		Index:       index,
		ID:          id,
		Version:     -1,
		Document:    make(map[string]interface{}),
	}
}

// Source returns the Elasticsearch bulk format for this request
func (br *BulkableRequest) Source() ([]string, error) {
	action := make(map[string]interface{})

	switch br.RequestType {
	case BulkableRequestTypeIndex:
		action["index"] = map[string]interface{}{
			"_index": br.Index,
			"_id":    br.ID,
		}
		if br.Version >= 0 {
			action["index"].(map[string]interface{})["_version"] = br.Version
		}
	case BulkableRequestTypeDelete:
		action["delete"] = map[string]interface{}{
			"_index": br.Index,
			"_id":    br.ID,
		}
		if br.Version >= 0 {
			action["delete"].(map[string]interface{})["_version"] = br.Version
		}
	}

	actionBytes, err := json.Marshal(action)
	if err != nil {
		return nil, err
	}

	result := []string{string(actionBytes)}

	// Add document for index requests
	if br.RequestType == BulkableRequestTypeIndex && len(br.Document) > 0 {
		docBytes, err := json.Marshal(br.Document)
		if err != nil {
			return nil, err
		}
		result = append(result, string(docBytes))
	}

	return result, nil
}

// SetSource sets the document source for the request (for compatibility)
func (br *BulkableRequest) SetSource(doc map[string]interface{}) *BulkableRequest {
	br.Document = doc
	return br
}

// Doc sets the document for the request (alias for SetSource for compatibility)
func (br *BulkableRequest) Doc(doc map[string]interface{}) *BulkableRequest {
	br.Document = doc
	return br
}

// Id sets the ID for the request (for compatibility)
func (br *BulkableRequest) Id(id string) *BulkableRequest {
	br.ID = id
	return br
}

// NewBulkDeleteRequest creates a new bulk delete request
func NewBulkDeleteRequest(index, id string) *BulkableRequest {
	return &BulkableRequest{
		RequestType: BulkableRequestTypeDelete,
		Index:       index,
		ID:          id,
		Version:     -1,
	}
}
