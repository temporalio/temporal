//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination bulk_processor_mock.go

package client

import (
	"time"

	"github.com/olivere/elastic/v7"
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

	// BulkProcessorParameters holds all required and optional parameters for executing bulk service
	BulkProcessorParameters struct {
		Name          string
		NumOfWorkers  int
		BulkActions   int
		BulkSize      int
		FlushInterval time.Duration
		BeforeFunc    elastic.BulkBeforeFunc
		AfterFunc     elastic.BulkAfterFunc
	}

	BulkableRequest struct {
		RequestType BulkableRequestType
		Index       string
		ID          string
		Version     int64
		Doc         map[string]interface{}
	}
)
