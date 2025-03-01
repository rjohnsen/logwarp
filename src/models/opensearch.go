package models

// IndexMeta represents metadata for an OpenSearch document.
type IndexMeta struct {
	DocumentID string `json:"_id"`    // Unique document identifier.
	IndexName  string `json:"_index"` // Index name where the document is stored.
}

// IndexMetadataWrapper wraps index metadata for bulk operations.
type IndexMetadataWrapper struct {
	Index IndexMeta `json:"index"`
}

// DocumentSource represents the actual content of an OpenSearch document.
type DocumentSource struct {
	Content map[string]interface{} `json:"_source"` // Document's main data.
}

// Document represents a complete OpenSearch document with metadata and source.
type Document struct {
	Metadata IndexMetadataWrapper // Index metadata.
	Source   DocumentSource       // Document content.
}
