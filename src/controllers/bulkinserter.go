package controllers

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/opensearch-project/opensearch-go"
	"github.com/rjohnsen/logwarp/src/models"
)

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
