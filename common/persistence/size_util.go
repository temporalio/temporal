package persistence

import (
	commonpb "go.temporal.io/api/common/v1"
)

func sizeOfBlob(
	blob *commonpb.DataBlob,
) int {
	return blob.Size()
}

func sizeOfInt64Set(
	int64Set map[int64]struct{},
) int {
	// 8 == 64 bit / 8 bit per byte
	return 8 * len(int64Set)
}

func sizeOfStringSet(
	stringSet map[string]struct{},
) int {
	size := 0
	for requestID := range stringSet {
		size += len(requestID)
	}
	return size
}

func sizeOfInt64BlobMap(
	kvBlob map[int64]*commonpb.DataBlob,
) int {
	// 8 == 64 bit / 8 bit per byte
	size := 8 * len(kvBlob)
	for _, blob := range kvBlob {
		size += blob.Size()
	}
	return size
}

// sizeOfChasmNodeMap is a special case since the persistence interface separates
// a node's metadata and data fields.
func sizeOfChasmNodeMap(
	nodeMap map[string]InternalChasmNode,
) int {
	size := 0
	for path, node := range nodeMap {
		size += len(path) + node.Metadata.Size() + node.Data.Size()
	}
	return size
}

func sizeOfStringBlobMap(
	kvBlob map[string]*commonpb.DataBlob,
) int {
	size := 0
	for id, blob := range kvBlob {
		// 8 == 64 bit / 8 bit per byte
		size += len(id) + blob.Size()
	}
	return size
}

func sizeOfStringSlice(
	stringSlice []string,
) int {
	size := 0
	for _, str := range stringSlice {
		size += len(str)
	}
	return size
}

func sizeOfBlobSlice(
	blobSlice []*commonpb.DataBlob,
) int {
	size := 0
	for _, blob := range blobSlice {
		size += blob.Size()
	}
	return size
}
