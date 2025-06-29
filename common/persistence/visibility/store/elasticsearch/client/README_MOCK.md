# ElasticClient Mock

This directory contains the mock implementation for the new `ElasticClient` interface.

## Files

- `elastic_client.go` - The clean interface definition
- `elastic_client_mock.go` - Generated mock (DO NOT EDIT manually)
- `elastic_client_mock_test.go` - Example tests showing mock usage
- `client_v8.go` - go-elasticsearch v8 implementation

## Regenerating Mocks

When you modify the `ElasticClient` interface, regenerate the mock:

```bash
cd common/persistence/visibility/store/elasticsearch/client/
mockgen -package client -source elastic_client.go -destination elastic_client_mock.go
```

## Using the Mock in Tests

```go
import (
    "testing"
    "context"
    "go.uber.org/mock/gomock"
)

func TestMyFunction(t *testing.T) {
    ctrl := gomock.NewController(t)
    defer ctrl.Finish()
    
    // Create mock
    mockClient := NewMockElasticClient(ctrl)
    
    // Set expectations  
    mockClient.EXPECT().
        IndexExists(gomock.Any(), "test-index").
        Return(true, nil)
    
    // Use mock in your code
    result := myFunction(mockClient)
    
    // Assert results...
}
```

## Migration from Legacy Mocks

The legacy `client_mock.go` and new `elastic_client_mock.go` **coexist** during the migration:

- **Legacy components** (like `VisibilityStore`) still use `client.Client` interface → use `NewMockClient()`
- **New components** using `ElasticClient` interface → use `NewMockElasticClient()`
