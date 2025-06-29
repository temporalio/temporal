package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"go.temporal.io/server/common/log"
)

// clientV8Impl implements ElasticClient interfaces
type clientV8Impl struct {
	esClient *elasticsearch.Client
	url      url.URL
}

var _ ElasticClient = (*clientV8Impl)(nil)

// newClientV8 creates a new ES client using go-elasticsearch v8
func newClientV8(cfg *Config, httpClient *http.Client, logger log.Logger) (*clientV8Impl, error) {
	var urls []string
	if len(cfg.URLs) > 0 {
		urls = make([]string, len(cfg.URLs))
		for i, u := range cfg.URLs {
			urls[i] = u.String()
		}
	} else {
		urls = []string{cfg.URL.String()}
	}

	if httpClient == nil {
		if cfg.TLS != nil && cfg.TLS.Enabled {
			tlsHttpClient, err := buildTLSHTTPClient(cfg.TLS)
			if err != nil {
				return nil, fmt.Errorf("unable to create TLS HTTP client: %w", err)
			}
			httpClient = tlsHttpClient
		} else {
			httpClient = http.DefaultClient
		}
	}

	esConfig := elasticsearch.Config{
		Addresses: urls,
		Username:  cfg.Username,
		Password:  cfg.Password,
		Transport: httpClient.Transport,
		Header:    make(http.Header),
	}

	if cfg.EnableSniff {
		esConfig.DiscoverNodesOnStart = true
		esConfig.DiscoverNodesInterval = 60 * time.Second
	}

	client, err := elasticsearch.NewClient(esConfig)
	if err != nil {
		return nil, err
	}

	return &clientV8Impl{
		esClient: client,
		url:      cfg.URL,
	}, nil
}

func (c *clientV8Impl) IndexExists(ctx context.Context, indexName string) (bool, error) {
	req := esapi.IndicesExistsRequest{
		Index: []string{indexName},
	}

	res, err := req.Do(ctx, c.esClient)
	if err != nil {
		return false, err
	}
	defer res.Body.Close()

	return res.StatusCode == 200, nil
}

func (c *clientV8Impl) Ping(ctx context.Context) error {
	req := esapi.InfoRequest{}
	res, err := req.Do(ctx, c.esClient)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("ping error: %s", res.String())
	}

	return nil
}

func (c *clientV8Impl) CreateIndex(ctx context.Context, index string, body map[string]any) (bool, error) {
	var bodyBytes []byte
	var err error

	if body != nil {
		bodyBytes, err = json.Marshal(body)
		if err != nil {
			return false, fmt.Errorf("failed to marshal request body: %w", err)
		}
	}

	req := esapi.IndicesCreateRequest{
		Index: index,
		Body:  bytes.NewReader(bodyBytes),
	}

	res, err := req.Do(ctx, c.esClient)
	if err != nil {
		return false, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return false, fmt.Errorf("create index error: %s", res.String())
	}

	var result struct {
		Acknowledged bool `json:"acknowledged"`
	}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return false, err
	}

	return result.Acknowledged, nil
}

func (c *clientV8Impl) DeleteIndex(ctx context.Context, indexName string) (bool, error) {
	req := esapi.IndicesDeleteRequest{
		Index: []string{indexName},
	}

	res, err := req.Do(ctx, c.esClient)
	if err != nil {
		return false, err
	}
	defer res.Body.Close()

	// Handle 404 as a successful deletion (index was already gone)
	if res.StatusCode == 404 {
		return true, nil
	}

	if res.IsError() {
		return false, fmt.Errorf("delete index error: %s", res.String())
	}

	var result struct {
		Acknowledged bool `json:"acknowledged"`
	}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return false, err
	}

	return result.Acknowledged, nil
}

func (c *clientV8Impl) GetDocument(ctx context.Context, index string, docID string) (*types.GetResult, error) {
	req := esapi.GetRequest{
		Index:      index,
		DocumentID: docID,
	}

	res, err := req.Do(ctx, c.esClient)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	// Handle 404 as document not found
	if res.StatusCode == 404 {
		return &types.GetResult{
			Index_: index,
			Id_:    docID,
			Found:  false,
		}, nil
	}

	if res.IsError() {
		return nil, fmt.Errorf("get document error: %s", res.String())
	}

	var result types.GetResult
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, err
	}

	return &result, nil
}
