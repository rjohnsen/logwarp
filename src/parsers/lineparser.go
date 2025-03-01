// Package parsers handles parsing and bulk indexing of documents into OpenSearch.
package parsers

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/opensearch-project/opensearch-go"
	"github.com/rjohnsen/logwarp/src/models"
)

// ParseLinesAndInsert reads a file line by line, parses each line as JSON,
// and performs bulk inserts into OpenSearch.
func LineParser(client *opensearch.Client, filePath string) {
	const bulkSize = 10000

	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var documentsBatch []models.Document

	for scanner.Scan() {
		line := scanner.Text()
		var rawJSON map[string]interface{}

		// Parse JSON line into a temporary map to extract both index and source data.
		if err := json.Unmarshal([]byte(line), &rawJSON); err != nil {
			log.Printf("Failed to parse JSON: %v", err)
			continue
		}

		// Extract index and source information into a Document struct.
		document := models.Document{
			Metadata: models.IndexMetadataWrapper{
				Index: models.IndexMeta{
					DocumentID: rawJSON["_id"].(string),
					IndexName:  rawJSON["_index"].(string),
				},
			},
			Source: models.DocumentSource{
				Content: rawJSON["_source"].(map[string]interface{}),
			},
		}

		documentsBatch = append(documentsBatch, document)

		// Perform bulk insert when the batch reaches bulkSize.
		if len(documentsBatch) >= bulkSize {
			if err := PerformBulkInsert(client, documentsBatch); err != nil {
				log.Printf("Bulk insert failed: %v", err)
			}
			documentsBatch = documentsBatch[:0] // Clear slice without reallocating.
		}
	}

	// Insert remaining documents if any.
	if len(documentsBatch) > 0 {
		if err := PerformBulkInsert(client, documentsBatch); err != nil {
			log.Printf("Bulk insert failed: %v", err)
		}
	}
}

// PerformBulkInsert performs a bulk insert of documents into OpenSearch.
func PerformBulkInsert(client *opensearch.Client, documents []models.Document) error {
	var bulkRequestPayload []string

	// Prepare bulk request payload.
	for _, document := range documents {
		indexMetadata, err := json.Marshal(document.Metadata)
		if err != nil {
			return fmt.Errorf("failed to marshal index metadata: %w", err)
		}
		documentContent, err := json.Marshal(document.Source.Content)
		if err != nil {
			return fmt.Errorf("failed to marshal document content: %w", err)
		}

		bulkRequestPayload = append(bulkRequestPayload, string(indexMetadata), string(documentContent))
	}

	// Create shipment payload for OpenSearch bulk API.
	bulkRequestBody := strings.Join(bulkRequestPayload, "\n") + "\n"
	bulkResponse, err := client.Bulk(strings.NewReader(bulkRequestBody))
	if err != nil {
		return fmt.Errorf("failed to execute bulk request: %w", err)
	}
	defer bulkResponse.Body.Close()

	// Check response status.
	if bulkResponse.StatusCode != 200 {
		return fmt.Errorf("bulk request failed with status: %d", bulkResponse.StatusCode)
	}

	// Parse response for success confirmation.
	var responseJSON map[string]interface{}
	if err := json.NewDecoder(bulkResponse.Body).Decode(&responseJSON); err != nil {
		return fmt.Errorf("failed to parse bulk response: %w", err)
	}

	log.Printf("Status: %v => Documents Indexed: %d", bulkResponse.StatusCode, len(documents))
	return nil
}
