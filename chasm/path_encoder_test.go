package chasm

import (
	"testing"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

func TestDefaultPathEncoder_EncodeDecode(t *testing.T) {
	e := &defaultPathEncoder{}

	root := &Node{
		nodeName: "",
		serializedNode: &persistencespb.ChasmNode{
			Metadata: &persistencespb.ChasmNodeMetadata{
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{ComponentAttributes: &persistencespb.ChasmComponentAttributes{}},
			},
		},
	}
	child := &Node{
		parent:   root,
		nodeName: "child",
		serializedNode: &persistencespb.ChasmNode{
			Metadata: &persistencespb.ChasmNodeMetadata{
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{ComponentAttributes: &persistencespb.ChasmComponentAttributes{}},
			},
		},
	}
	collection := &Node{
		parent:   root,
		nodeName: "collection",
		serializedNode: &persistencespb.ChasmNode{
			Metadata: &persistencespb.ChasmNodeMetadata{
				Attributes: &persistencespb.ChasmNodeMetadata_CollectionAttributes{CollectionAttributes: &persistencespb.ChasmCollectionAttributes{}},
			},
		},
	}
	collectionItem := &Node{
		parent:   collection,
		nodeName: "item",
		serializedNode: &persistencespb.ChasmNode{
			Metadata: &persistencespb.ChasmNodeMetadata{
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{ComponentAttributes: &persistencespb.ChasmComponentAttributes{}},
			},
		},
	}
	collectionItemData := &Node{
		parent:   collectionItem,
		nodeName: "data",
		serializedNode: &persistencespb.ChasmNode{
			Metadata: &persistencespb.ChasmNodeMetadata{
				Attributes: &persistencespb.ChasmNodeMetadata_DataAttributes{},
			},
		},
	}

	tests := []struct {
		node    *Node
		path    []string
		encoded string
	}{
		{root, []string{}, ""},

		{child, []string{"child"}, "child"},
		{child, []string{"special\\#"}, "special\\\\\\#"},
		{child, []string{" !"}, "\\ \\!"},
		{child, []string{"你好"}, "你好"},

		{collection, []string{"collection"}, "collection"},

		{collectionItem, []string{"collection", "item"}, "collection#item"},
		{collectionItem, []string{"collection", "⌘"}, "collection#⌘"},

		{collectionItemData, []string{"collection", "item", "data"}, "collection$item$data"},
		{collectionItemData, []string{"collection", "item", "世界"}, "collection$item$世界"},
	}

	for _, tt := range tests {
		encoded, err := e.Encode(tt.node, tt.path)
		require.NoError(t, err)
		require.Equal(t, tt.encoded, encoded)

		decodedPath, err := e.Decode(encoded)
		require.NoError(t, err)
		require.Equal(t, tt.path, decodedPath)
	}
}
