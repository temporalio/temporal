package client

import (
	"context"
	"errors"
	"testing"

	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"go.uber.org/mock/gomock"
)

// Example test showing how to use the new ElasticClient mock
func TestElasticClientMock_Example(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create mock client
	mockClient := NewMockElasticClient(ctrl)
	ctx := context.Background()

	// Test successful ping
	t.Run("Ping Success", func(t *testing.T) {
		mockClient.EXPECT().
			Ping(ctx).
			Return(nil)

		err := mockClient.Ping(ctx)
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
	})

	// Test ping failure
	t.Run("Ping Failure", func(t *testing.T) {
		expectedErr := errors.New("connection failed")
		mockClient.EXPECT().
			Ping(ctx).
			Return(expectedErr)

		err := mockClient.Ping(ctx)
		if err != expectedErr {
			t.Errorf("Expected %v, got %v", expectedErr, err)
		}
	})

	// Test IndexExists
	t.Run("IndexExists", func(t *testing.T) {
		mockClient.EXPECT().
			IndexExists(ctx, "test-index").
			Return(true, nil)

		exists, err := mockClient.IndexExists(ctx, "test-index")
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		if !exists {
			t.Error("Expected index to exist")
		}
	})

	// Test CreateIndex
	t.Run("CreateIndex", func(t *testing.T) {
		indexBody := map[string]any{
			"settings": map[string]any{
				"number_of_shards": 1,
			},
		}

		mockClient.EXPECT().
			CreateIndex(ctx, "test-index", indexBody).
			Return(true, nil)

		acknowledged, err := mockClient.CreateIndex(ctx, "test-index", indexBody)
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		if !acknowledged {
			t.Error("Expected create to be acknowledged")
		}
	})

	// Test DeleteIndex
	t.Run("DeleteIndex", func(t *testing.T) {
		mockClient.EXPECT().
			DeleteIndex(ctx, "test-index").
			Return(true, nil)

		acknowledged, err := mockClient.DeleteIndex(ctx, "test-index")
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		if !acknowledged {
			t.Error("Expected delete to be acknowledged")
		}
	})

	// Test GetDocument
	t.Run("GetDocument", func(t *testing.T) {
		expectedResult := &types.GetResult{
			Index_: "test-index",
			Id_:    "doc1",
			Found:  true,
		}

		mockClient.EXPECT().
			GetDocument(ctx, "test-index", "doc1").
			Return(expectedResult, nil)

		result, err := mockClient.GetDocument(ctx, "test-index", "doc1")
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		if result.Index_ != "test-index" {
			t.Errorf("Expected index 'test-index', got %s", result.Index_)
		}
		if !result.Found {
			t.Error("Expected document to be found")
		}
	})
}

// Example test showing call ordering and argument matching
func TestElasticClientMock_Advanced(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := NewMockElasticClient(ctrl)
	ctx := context.Background()

	t.Run("Call Ordering", func(t *testing.T) {
		// Define expected call order
		gomock.InOrder(
			mockClient.EXPECT().IndexExists(ctx, "test-index").Return(false, nil),
			mockClient.EXPECT().CreateIndex(ctx, "test-index", gomock.Any()).Return(true, nil),
			mockClient.EXPECT().IndexExists(ctx, "test-index").Return(true, nil),
		)

		// Execute in order
		exists, _ := mockClient.IndexExists(ctx, "test-index")
		if exists {
			t.Error("Expected index not to exist initially")
		}

		acknowledged, _ := mockClient.CreateIndex(ctx, "test-index", map[string]any{})
		if !acknowledged {
			t.Error("Expected create to be acknowledged")
		}

		exists, _ = mockClient.IndexExists(ctx, "test-index")
		if !exists {
			t.Error("Expected index to exist after creation")
		}
	})

	t.Run("Argument Matching", func(t *testing.T) {
		// Match any context and specific index name
		mockClient.EXPECT().
			IndexExists(gomock.Any(), "specific-index").
			Return(true, nil)

		// Match any arguments
		mockClient.EXPECT().
			CreateIndex(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(true, nil)

		// Match with custom matcher
		mockClient.EXPECT().
			GetDocument(gomock.Any(), gomock.Any(), gomock.Not(gomock.Eq(""))).
			Return(&types.GetResult{Found: false}, nil)

		// Test the expectations
		mockClient.IndexExists(ctx, "specific-index")
		mockClient.CreateIndex(ctx, "any-index", nil)
		mockClient.GetDocument(ctx, "any-index", "non-empty-id")
	})

	t.Run("Multiple Return Values", func(t *testing.T) {
		// Return different values on subsequent calls
		mockClient.EXPECT().
			IndexExists(ctx, "flaky-index").
			Return(false, nil).
			Times(1)

		mockClient.EXPECT().
			IndexExists(ctx, "flaky-index").
			Return(true, nil).
			Times(1)

		// First call returns false
		exists, _ := mockClient.IndexExists(ctx, "flaky-index")
		if exists {
			t.Error("Expected first call to return false")
		}

		// Second call returns true
		exists, _ = mockClient.IndexExists(ctx, "flaky-index")
		if !exists {
			t.Error("Expected second call to return true")
		}
	})
}
