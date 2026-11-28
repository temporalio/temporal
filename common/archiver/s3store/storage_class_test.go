package s3store

import (
	"bytes"
	"cmp"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	archiverspb "go.temporal.io/server/api/archiver/v1"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/s3store/mocks"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestArchiverRejectsInvalidStorageClass(t *testing.T) {
	t.Parallel()
	s3config := &config.S3Archiver{Region: "us-east-1", StorageClass: "INVALID"}
	t.Run("history", func(t *testing.T) {
		t.Parallel()
		_, err := NewHistoryArchiver(nil, log.NewNoopLogger(), metrics.NoopMetricsHandler, s3config)
		require.ErrorContains(t, err, `invalid S3 storageClass "INVALID"`)
	})
	t.Run("visibility", func(t *testing.T) {
		t.Parallel()
		_, err := NewVisibilityArchiver(log.NewNoopLogger(), metrics.NoopMetricsHandler, s3config)
		require.ErrorContains(t, err, `invalid S3 storageClass "INVALID"`)
	})
}

func TestHistoryArchiveStorageClass(t *testing.T) {
	t.Parallel()
	for _, storageClass := range []string{"", "STANDARD_IA"} {
		t.Run(cmp.Or(storageClass, "default"), func(t *testing.T) {
			t.Parallel()
			controller := gomock.NewController(t)
			s3cli := mocks.NewMockS3API(controller)
			iterator := archiver.NewMockHistoryIterator(controller)
			blob := &archiverspb.HistoryBlob{
				Header: &archiverspb.HistoryBlobHeader{IsLast: true},
				Body: []*historypb.History{{Events: []*historypb.HistoryEvent{{
					EventId: 1,
					Version: 100,
				}}}},
			}
			gomock.InOrder(
				iterator.EXPECT().HasNext().Return(true),
				iterator.EXPECT().Next(gomock.Any()).Return(blob, nil),
				iterator.EXPECT().HasNext().Return(false),
			)
			archivalURI, err := archiver.NewURI("s3://bucket/path")
			require.NoError(t, err)
			s3cli.EXPECT().HeadObject(gomock.Any(), gomock.Any()).Return(nil, &types.NotFound{})
			data, err := Encode(blob)
			require.NoError(t, err)
			s3cli.EXPECT().PutObject(gomock.Any(), &s3.PutObjectInput{
				Bucket:       aws.String("bucket"),
				Key:          aws.String(constructHistoryKey("/path", "namespace-id", "workflow-id", "run-id", 100, 0)),
				Body:         bytes.NewReader(data),
				StorageClass: types.StorageClass(storageClass),
			}).Return(&s3.PutObjectOutput{}, nil)
			h, err := newHistoryArchiver(nil, log.NewNoopLogger(), metrics.NoopMetricsHandler,
				&config.S3Archiver{Region: "us-east-1", StorageClass: storageClass}, iterator)
			require.NoError(t, err)
			h.s3cli = s3cli
			require.NoError(t, h.Archive(t.Context(), archivalURI, &archiver.ArchiveHistoryRequest{
				NamespaceID:          "namespace-id",
				Namespace:            "namespace",
				WorkflowID:           "workflow-id",
				RunID:                "run-id",
				NextEventID:          2,
				CloseFailoverVersion: 100,
			}))
		})
	}
}

func TestVisibilityArchiveStorageClass(t *testing.T) {
	t.Parallel()
	for _, storageClass := range []string{"", "STANDARD_IA"} {
		t.Run(cmp.Or(storageClass, "default"), func(t *testing.T) {
			t.Parallel()
			for _, deduplicate := range []bool{false, true} {
				name := "normal"
				if deduplicate {
					name = "deduplicated"
				}
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					s3cli := mocks.NewMockS3API(gomock.NewController(t))
					archivalURI, err := archiver.NewURI("s3://bucket/path")
					require.NoError(t, err)
					closeTime := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
					record := &archiverspb.VisibilityRecord{
						NamespaceId:      "namespace-id",
						Namespace:        "namespace",
						WorkflowId:       "workflow-id",
						RunId:            "run-id",
						WorkflowTypeName: "workflow-type",
						StartTime:        timestamppb.New(closeTime.Add(-time.Hour)),
						CloseTime:        timestamppb.New(closeTime),
						Status:           enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
					}
					data, err := Encode(record)
					require.NoError(t, err)
					var metadata map[string]string
					var options []archiver.ArchiveOption
					if deduplicate {
						hash, err := archiver.VisibilityArchivalRecordHash(record)
						require.NoError(t, err)
						metadata = map[string]string{archiver.VisibilityArchivalRecordHashMetadataKey: hash}
						options = append(options, archiver.GetVisibilityArchivalRecordDeduplicationOption())
						s3cli.EXPECT().HeadObject(gomock.Any(), gomock.Any()).Return(nil, &types.NotFound{}).Times(4)
					}
					for _, index := range createIndexesToArchive(record) {
						key := constructTimestampIndex("/path", record.NamespaceId, index.primaryIndex, index.primaryIndexValue,
							index.secondaryIndex, index.secondaryIndexTimestamp, record.RunId)
						s3cli.EXPECT().PutObject(gomock.Any(), &s3.PutObjectInput{
							Bucket:       aws.String("bucket"),
							Key:          aws.String(key),
							Body:         bytes.NewReader(data),
							Metadata:     metadata,
							StorageClass: types.StorageClass(storageClass),
						}).Return(&s3.PutObjectOutput{}, nil)
					}
					v, err := newVisibilityArchiver(log.NewNoopLogger(), metrics.NoopMetricsHandler,
						&config.S3Archiver{Region: "us-east-1", StorageClass: storageClass})
					require.NoError(t, err)
					v.s3cli = s3cli
					require.NoError(t, v.Archive(t.Context(), archivalURI, record, options...))
				})
			}
		})
	}
}
