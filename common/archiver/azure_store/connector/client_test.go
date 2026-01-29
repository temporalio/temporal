package connector

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/config"
	"go.uber.org/mock/gomock"
)

func TestFormatBlobPath(t *testing.T) {
	tests := []struct {
		path     string
		fileName string
		expected string
	}{
		{
			path:     "",
			fileName: "file.txt",
			expected: "file.txt",
		},
		{
			path:     "/",
			fileName: "file.txt",
			expected: "file.txt",
		},
		{
			path:     "/path",
			fileName: "file.txt",
			expected: "path/file.txt",
		},
		{
			path:     "path/",
			fileName: "file.txt",
			expected: "path/file.txt",
		},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.expected, formatBlobPath(tt.path, tt.fileName))
	}
}

func TestNewClient(t *testing.T) {
	cfg := &config.AzblobArchiver{
		AccountName:  "test",
		AccountKey:   "test",
		Endpoint:     "https://test.blob.core.windows.net",
		UseSharedKey: true,
	}
	client, err := NewClient(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, client)
}

func TestMockClient(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := NewMockClient(ctrl)
	ctx := context.Background()
	uri, _ := archiver.NewURI("azblob://my-container/path")
	fileName := "test.history"
	data := []byte("test data")

	mockClient.EXPECT().Upload(ctx, uri, fileName, data).Return(nil)
	err := mockClient.Upload(ctx, uri, fileName, data)
	assert.NoError(t, err)

	mockClient.EXPECT().Get(ctx, uri, fileName).Return(data, nil)
	resp, err := mockClient.Get(ctx, uri, fileName)
	assert.NoError(t, err)
	assert.Equal(t, data, resp)
}
