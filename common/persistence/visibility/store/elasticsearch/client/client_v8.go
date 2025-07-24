package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/blang/semver/v4"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/log"
)

type (
	// clientV8Impl implements Client using the official Elasticsearch Go client v8
	clientV8Impl struct {
		esClient       *elasticsearch.Client
		typedAPI       *elasticsearch.TypedClient
		logger         log.Logger
		initOnce       sync.Once
		isPitSupported bool
	}
)

const (
	pointInTimeSupportedFlavorV8 = "default"
)

var (
	pointInTimeSupportedInV8 = semver.MustParseRange(">=7.10.0")
)

var _ Client = (*clientV8Impl)(nil)

// newClientV8 creates a new Elasticsearch client using the official v8 client
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

	esConfig := elasticsearch.Config{
		Addresses: urls,
		Username:  cfg.Username,
		Password:  cfg.Password,
		Transport: httpClient.Transport,
	}

	// Add AWS signing if configured
	if httpClient != nil {
		esConfig.Transport = httpClient.Transport
	} else if cfg.TLS != nil && cfg.TLS.Enabled {
		tlsHttpClient, err := buildTLSHTTPClient(cfg.TLS)
		if err != nil {
			return nil, fmt.Errorf("unable to create TLS HTTP client: %w", err)
		}
		esConfig.Transport = tlsHttpClient.Transport
	}

	client, err := elasticsearch.NewClient(esConfig)
	if err != nil {
		return nil, err
	}

	typedClient, err := elasticsearch.NewTypedClient(esConfig)
	if err != nil {
		return nil, err
	}

	return &clientV8Impl{
		esClient: client,
		typedAPI: typedClient,
		logger:   logger,
	}, nil
}

func (c *clientV8Impl) Get(ctx context.Context, index string, docID string) (*GetResult, error) {
	res, err := c.esClient.Get(index, docID, c.esClient.Get.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("Elasticsearch error: %s", res.String())
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	var result GetResult
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}

	return &result, nil
}

func (c *clientV8Impl) Search(ctx context.Context, p *SearchParameters) (*SearchResult, error) {
	// Convert query to JSON
	queryJSON, err := p.Query.Source()
	if err != nil {
		return nil, err
	}

	// Convert sorters to JSON
	var sortJSON []interface{}
	for _, sorter := range p.Sorter {
		sortSource, err := sorter.Source()
		if err != nil {
			return nil, err
		}
		sortJSON = append(sortJSON, sortSource)
	}

	// Build search request
	searchBody := map[string]interface{}{
		"query":            queryJSON,
		"track_total_hits": false,
	}

	if len(sortJSON) > 0 {
		searchBody["sort"] = sortJSON
	}

	if p.PageSize > 0 {
		searchBody["size"] = p.PageSize
	}

	if len(p.SearchAfter) > 0 {
		searchBody["search_after"] = p.SearchAfter
	}

	if p.PointInTime != nil {
		searchBody["pit"] = map[string]interface{}{
			"id":         p.PointInTime.Id,
			"keep_alive": p.PointInTime.KeepAlive,
		}
	}

	body, err := json.Marshal(searchBody)
	if err != nil {
		return nil, err
	}

	var options []func(*esapi.SearchRequest)
	options = append(options, c.esClient.Search.WithContext(ctx))
	options = append(options, c.esClient.Search.WithBody(bytes.NewReader(body)))

	// Only set index if not using point-in-time
	if p.PointInTime == nil {
		options = append(options, c.esClient.Search.WithIndex(p.Index))
	}

	res, err := c.esClient.Search(options...)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("Elasticsearch search error: %s", res.String())
	}

	return c.parseSearchResponse(res.Body)
}

func (c *clientV8Impl) parseSearchResponse(body io.Reader) (*SearchResult, error) {
	respBody, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}

	var esResp struct {
		Hits struct {
			Total struct {
				Value int64 `json:"value"`
			} `json:"total"`
			Hits []struct {
				Index  string          `json:"_index"`
				Type   string          `json:"_type"`
				Id     string          `json:"_id"`
				Score  *float64        `json:"_score"`
				Source json.RawMessage `json:"_source"`
				Sort   []interface{}   `json:"sort,omitempty"`
			} `json:"hits"`
		} `json:"hits"`
		ScrollId      string                     `json:"_scroll_id,omitempty"`
		PointInTimeId string                     `json:"pit_id,omitempty"`
		Aggregations  map[string]json.RawMessage `json:"aggregations,omitempty"`
	}

	if err := json.Unmarshal(respBody, &esResp); err != nil {
		return nil, err
	}

	hits := make([]*SearchHit, len(esResp.Hits.Hits))
	for i, hit := range esResp.Hits.Hits {
		hits[i] = &SearchHit{
			Index:  hit.Index,
			Type:   hit.Type,
			Id:     hit.Id,
			Score:  hit.Score,
			Source: hit.Source,
			Sort:   hit.Sort,
		}
	}

	return &SearchResult{
		TotalHits:     esResp.Hits.Total.Value,
		Hits:          hits,
		ScrollId:      esResp.ScrollId,
		PointInTimeId: esResp.PointInTimeId,
		Aggregations:  convertAggregations(esResp.Aggregations),
	}, nil
}

func (c *clientV8Impl) Count(ctx context.Context, index string, query Query) (int64, error) {
	queryJSON, err := query.Source()
	if err != nil {
		return 0, err
	}

	body, err := json.Marshal(map[string]interface{}{
		"query": queryJSON,
	})
	if err != nil {
		return 0, err
	}

	res, err := c.esClient.Count(
		c.esClient.Count.WithContext(ctx),
		c.esClient.Count.WithIndex(index),
		c.esClient.Count.WithBody(bytes.NewReader(body)),
	)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return 0, fmt.Errorf("Elasticsearch count error: %s", res.String())
	}

	respBody, err := io.ReadAll(res.Body)
	if err != nil {
		return 0, err
	}

	var countResp struct {
		Count int64 `json:"count"`
	}

	if err := json.Unmarshal(respBody, &countResp); err != nil {
		return 0, err
	}

	return countResp.Count, nil
}

func (c *clientV8Impl) CountGroupBy(ctx context.Context, index string, query Query, aggName string, agg Aggregation) (*SearchResult, error) {
	queryJSON, err := query.Source()
	if err != nil {
		return nil, err
	}

	aggJSON, err := agg.Source()
	if err != nil {
		return nil, err
	}

	searchBody := map[string]interface{}{
		"query": queryJSON,
		"aggs": map[string]interface{}{
			aggName: aggJSON,
		},
		"size": 0, // Don't return actual hits for aggregation queries
	}

	body, err := json.Marshal(searchBody)
	if err != nil {
		return nil, err
	}

	res, err := c.esClient.Search(
		c.esClient.Search.WithContext(ctx),
		c.esClient.Search.WithIndex(index),
		c.esClient.Search.WithBody(bytes.NewReader(body)),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("Elasticsearch search error: %s", res.String())
	}

	return c.parseSearchResponse(res.Body)
}

func (c *clientV8Impl) RunBulkProcessor(ctx context.Context, p *BulkProcessorParameters) (BulkProcessor, error) {
	return newBulkProcessorV8(c.esClient, p, c.logger)
}

// buildMappingBody creates an Elasticsearch mapping body from the given field types
// This function is used by tests and is extracted from the PutMapping logic
func buildMappingBody(mapping map[string]enumspb.IndexedValueType) map[string]interface{} {
	properties := make(map[string]interface{})
	for field, valueType := range mapping {
		switch valueType {
		case enumspb.INDEXED_VALUE_TYPE_TEXT:
			properties[field] = map[string]interface{}{"type": "text"}
		case enumspb.INDEXED_VALUE_TYPE_KEYWORD:
			properties[field] = map[string]interface{}{"type": "keyword"}
		case enumspb.INDEXED_VALUE_TYPE_INT:
			properties[field] = map[string]interface{}{"type": "long"}
		case enumspb.INDEXED_VALUE_TYPE_DOUBLE:
			properties[field] = map[string]interface{}{
				"type":           "scaled_float",
				"scaling_factor": 10000,
			}
		case enumspb.INDEXED_VALUE_TYPE_BOOL:
			properties[field] = map[string]interface{}{"type": "boolean"}
		case enumspb.INDEXED_VALUE_TYPE_DATETIME:
			properties[field] = map[string]interface{}{"type": "date_nanos"}
			// Skip unknown types - they won't be added to properties
		}
	}

	return map[string]interface{}{
		"properties": properties,
	}
}

func (c *clientV8Impl) PutMapping(ctx context.Context, index string, mapping map[string]enumspb.IndexedValueType) (bool, error) {
	mappingBody := buildMappingBody(mapping)

	body, err := json.Marshal(mappingBody)
	if err != nil {
		return false, err
	}

	res, err := c.esClient.Indices.PutMapping(
		[]string{index},
		bytes.NewReader(body),
		c.esClient.Indices.PutMapping.WithContext(ctx),
	)
	if err != nil {
		return false, err
	}
	defer res.Body.Close()

	return !res.IsError(), nil
}

func (c *clientV8Impl) WaitForYellowStatus(ctx context.Context, index string) (string, error) {
	res, err := c.esClient.Cluster.Health(
		c.esClient.Cluster.Health.WithContext(ctx),
		c.esClient.Cluster.Health.WithIndex(index),
		c.esClient.Cluster.Health.WithWaitForStatus("yellow"),
		c.esClient.Cluster.Health.WithTimeout(30*time.Second),
	)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return "", err
	}

	if res.IsError() {
		return "", fmt.Errorf("Elasticsearch cluster health error: %s", string(body))
	}

	var healthResp struct {
		Status string `json:"status"`
	}

	if err := json.Unmarshal(body, &healthResp); err != nil {
		return "", err
	}

	return healthResp.Status, nil
}

func (c *clientV8Impl) GetMapping(ctx context.Context, index string) (map[string]string, error) {
	res, err := c.esClient.Indices.GetMapping(
		c.esClient.Indices.GetMapping.WithContext(ctx),
		c.esClient.Indices.GetMapping.WithIndex(index),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("Elasticsearch get mapping error: %s", res.String())
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	var mappings map[string]interface{}
	if err := json.Unmarshal(body, &mappings); err != nil {
		return nil, err
	}

	// Simplified mapping extraction - may need refinement based on actual usage
	result := make(map[string]string)
	for _, indexMapping := range mappings {
		if mapping, ok := indexMapping.(map[string]interface{}); ok {
			if mappingsField, exists := mapping["mappings"]; exists {
				if mappingsMap, ok := mappingsField.(map[string]interface{}); ok {
					if properties, exists := mappingsMap["properties"]; exists {
						if props, ok := properties.(map[string]interface{}); ok {
							for field, fieldMapping := range props {
								if fieldMap, ok := fieldMapping.(map[string]interface{}); ok {
									if fieldType, exists := fieldMap["type"]; exists {
										if typeStr, ok := fieldType.(string); ok {
											result[field] = typeStr
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}

	return result, nil
}

func (c *clientV8Impl) IndexExists(ctx context.Context, indexName string) (bool, error) {
	res, err := c.esClient.Indices.Exists(
		[]string{indexName},
		c.esClient.Indices.Exists.WithContext(ctx),
	)
	if err != nil {
		return false, err
	}
	defer res.Body.Close()

	return res.StatusCode == 200, nil
}

func (c *clientV8Impl) CreateIndex(ctx context.Context, index string, body map[string]any) (bool, error) {
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return false, err
	}

	res, err := c.esClient.Indices.Create(
		index,
		c.esClient.Indices.Create.WithContext(ctx),
		c.esClient.Indices.Create.WithBody(bytes.NewReader(bodyBytes)),
	)
	if err != nil {
		return false, err
	}
	defer res.Body.Close()

	return !res.IsError(), nil
}

func (c *clientV8Impl) DeleteIndex(ctx context.Context, indexName string) (bool, error) {
	res, err := c.esClient.Indices.Delete(
		[]string{indexName},
		c.esClient.Indices.Delete.WithContext(ctx),
	)
	if err != nil {
		return false, err
	}
	defer res.Body.Close()

	return !res.IsError(), nil
}

func (c *clientV8Impl) CatIndices(ctx context.Context, target string) (CatIndicesResponse, error) {
	var indices []string
	if target != "" {
		indices = []string{target}
	}

	res, err := c.esClient.Cat.Indices(
		c.esClient.Cat.Indices.WithContext(ctx),
		c.esClient.Cat.Indices.WithIndex(indices...),
		c.esClient.Cat.Indices.WithFormat("json"),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("Elasticsearch cat indices error: %s", res.String())
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	var catResponse []map[string]interface{}
	if err := json.Unmarshal(body, &catResponse); err != nil {
		return nil, err
	}

	return CatIndicesResponse(catResponse), nil
}

func (c *clientV8Impl) OpenScroll(ctx context.Context, p *SearchParameters, keepAliveInterval string) (*SearchResult, error) {
	queryJSON, err := p.Query.Source()
	if err != nil {
		return nil, err
	}

	var sortJSON []interface{}
	for _, sorter := range p.Sorter {
		sortSource, err := sorter.Source()
		if err != nil {
			return nil, err
		}
		sortJSON = append(sortJSON, sortSource)
	}

	searchBody := map[string]interface{}{
		"query": queryJSON,
	}

	if len(sortJSON) > 0 {
		searchBody["sort"] = sortJSON
	}

	body, err := json.Marshal(searchBody)
	if err != nil {
		return nil, err
	}

	var options []func(*esapi.SearchRequest)
	options = append(options, c.esClient.Search.WithContext(ctx))
	options = append(options, c.esClient.Search.WithIndex(p.Index))
	options = append(options, c.esClient.Search.WithBody(bytes.NewReader(body)))
	options = append(options, c.esClient.Search.WithScroll(time.Duration(time.Minute))) // Default scroll timeout

	if p.PageSize > 0 {
		options = append(options, c.esClient.Search.WithSize(p.PageSize))
	}

	res, err := c.esClient.Search(options...)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("Elasticsearch scroll search error: %s", res.String())
	}

	return c.parseSearchResponse(res.Body)
}

func (c *clientV8Impl) Scroll(ctx context.Context, id string, keepAliveInterval string) (*SearchResult, error) {
	res, err := c.esClient.Scroll(
		c.esClient.Scroll.WithContext(ctx),
		c.esClient.Scroll.WithScrollID(id),
		c.esClient.Scroll.WithScroll(time.Duration(time.Minute)),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("Elasticsearch scroll error: %s", res.String())
	}

	return c.parseSearchResponse(res.Body)
}

func (c *clientV8Impl) CloseScroll(ctx context.Context, id string) error {
	res, err := c.esClient.ClearScroll(
		c.esClient.ClearScroll.WithContext(ctx),
		c.esClient.ClearScroll.WithScrollID(id),
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("Elasticsearch clear scroll error: %s", res.String())
	}

	return nil
}

func (c *clientV8Impl) IsPointInTimeSupported(ctx context.Context) bool {
	c.initOnce.Do(func() {
		c.isPitSupported = c.queryPointInTimeSupported(ctx)
	})
	return c.isPitSupported
}

func (c *clientV8Impl) queryPointInTimeSupported(ctx context.Context) bool {
	res, err := c.esClient.Info(c.esClient.Info.WithContext(ctx))
	if err != nil {
		return false
	}
	defer res.Body.Close()

	if res.IsError() {
		return false
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return false
	}

	var info struct {
		Version struct {
			Number        string `json:"number"`
			BuildFlavor   string `json:"build_flavor"`
			LuceneVersion string `json:"lucene_version"`
		} `json:"version"`
	}

	if err := json.Unmarshal(body, &info); err != nil {
		return false
	}

	if info.Version.BuildFlavor != pointInTimeSupportedFlavorV8 {
		return false
	}

	version, err := semver.ParseTolerant(info.Version.Number)
	if err != nil {
		return false
	}

	return pointInTimeSupportedInV8(version)
}

func (c *clientV8Impl) OpenPointInTime(ctx context.Context, index string, keepAliveInterval string) (string, error) {
	req := esapi.OpenPointInTimeRequest{
		Index:     []string{index},
		KeepAlive: keepAliveInterval,
	}

	res, err := req.Do(ctx, c.esClient)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()

	if res.IsError() {
		return "", fmt.Errorf("Elasticsearch open point in time error: %s", res.String())
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return "", err
	}

	var pitResp struct {
		ID string `json:"id"`
	}

	if err := json.Unmarshal(body, &pitResp); err != nil {
		return "", err
	}

	return pitResp.ID, nil
}

func (c *clientV8Impl) ClosePointInTime(ctx context.Context, id string) (bool, error) {
	body := map[string]interface{}{
		"id": id,
	}

	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return false, err
	}

	res, err := c.esClient.ClosePointInTime(
		c.esClient.ClosePointInTime.WithContext(ctx),
		c.esClient.ClosePointInTime.WithBody(bytes.NewReader(bodyBytes)),
	)
	if err != nil {
		return false, err
	}
	defer res.Body.Close()

	return !res.IsError(), nil
}

// Delete implements CLIClient.Delete
func (c *clientV8Impl) Delete(ctx context.Context, indexName string, docID string, version int64) error {
	req := esapi.DeleteRequest{
		Index:       indexName,
		DocumentID:  docID,
		VersionType: versionTypeExternal,
		Version:     func() *int { v := int(version); return &v }(),
	}

	res, err := req.Do(ctx, c.esClient)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("Elasticsearch error: %s", res.String())
	}

	return nil
}

// IndexPutTemplate implements IntegrationTestsClient.IndexPutTemplate
func (c *clientV8Impl) IndexPutTemplate(ctx context.Context, templateName string, bodyString string) (bool, error) {
	req := esapi.IndicesPutTemplateRequest{
		Name: templateName,
		Body: bytes.NewReader([]byte(bodyString)),
	}

	res, err := req.Do(ctx, c.esClient)
	if err != nil {
		return false, err
	}
	defer res.Body.Close()

	return !res.IsError(), nil
}

// IndexPutSettings implements IntegrationTestsClient.IndexPutSettings
func (c *clientV8Impl) IndexPutSettings(ctx context.Context, indexName string, bodyString string) (bool, error) {
	req := esapi.IndicesPutSettingsRequest{
		Index: []string{indexName},
		Body:  bytes.NewReader([]byte(bodyString)),
	}

	res, err := req.Do(ctx, c.esClient)
	if err != nil {
		return false, err
	}
	defer res.Body.Close()

	return !res.IsError(), nil
}

// IndexGetSettings implements IntegrationTestsClient.IndexGetSettings
func (c *clientV8Impl) IndexGetSettings(ctx context.Context, indexName string) (map[string]*IndicesGetSettingsResponse, error) {
	req := esapi.IndicesGetSettingsRequest{
		Index: []string{indexName},
	}

	res, err := req.Do(ctx, c.esClient)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("Elasticsearch error: %s", res.String())
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	var response map[string]*IndicesGetSettingsResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, err
	}

	return response, nil
}

// Ping implements IntegrationTestsClient.Ping
func (c *clientV8Impl) Ping(ctx context.Context) error {
	res, err := c.esClient.Ping(c.esClient.Ping.WithContext(ctx))
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("Elasticsearch ping failed: %s", res.String())
	}

	return nil
}

// convertAggregations converts json.RawMessage aggregations to interface{} for compatibility
func convertAggregations(aggs map[string]json.RawMessage) map[string]interface{} {
	if aggs == nil {
		return nil
	}
	result := make(map[string]interface{})
	for k, v := range aggs {
		var decoded interface{}
		if err := json.Unmarshal(v, &decoded); err == nil {
			result[k] = decoded
		} else {
			result[k] = v
		}
	}
	return result
}

// convertToInterface converts json.RawMessage to interface{} for compatibility
func convertToInterface(raw json.RawMessage) interface{} {
	if raw == nil {
		return nil
	}
	var result interface{}
	if err := json.Unmarshal(raw, &result); err == nil {
		return result
	}
	return raw
}

// CatIndicesResponse type for compatibility

// buildTLSHTTPClient creates an HTTP client with TLS configuration
func buildTLSHTTPClient(config *auth.TLS) (*http.Client, error) {
	tlsConfig, err := auth.NewTLSConfig(config)
	if err != nil {
		return nil, err
	}

	transport := &http.Transport{TLSClientConfig: tlsConfig}
	tlsClient := &http.Client{Transport: transport}

	return tlsClient, nil
}
