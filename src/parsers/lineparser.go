// Package parsers handles parsing and bulk indexing of documents into OpenSearch.
package parsers

import (
	"bufio"
	"encoding/json"
	"log"
	"os"

	"github.com/opensearch-project/opensearch-go"
	"github.com/rjohnsen/logwarp/src/controllers"
	"github.com/rjohnsen/logwarp/src/models"
)

// ElasticParser reads a file line by line, parses each line as JSON,
// and performs bulk inserts into OpenSearch.
func ElasticParser(client *opensearch.Client, index string, filePath string) {
	const bulkSize = 5000

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
		if len(index) == 0 {
			index = rawJSON["_index"].(string)
		}

		document := models.Document{
			Metadata: models.IndexMetadataWrapper{
				Index: models.IndexMeta{
					DocumentID: rawJSON["_id"].(string),
					IndexName:  index,
				},
			},
			Source: models.DocumentSource{
				Content: rawJSON["_source"].(map[string]interface{}),
			},
		}

		documentsBatch = append(documentsBatch, document)

		// Perform bulk insert when the batch reaches bulkSize.
		if len(documentsBatch) >= bulkSize {
			if err := controllers.PerformBulkInsert(client, documentsBatch); err != nil {
				log.Printf("Bulk insert failed: %v", err)
			}
			documentsBatch = documentsBatch[:0] // Clear slice without reallocating.
		}
	}

	// Insert remaining documents if any.
	if len(documentsBatch) > 0 {
		if err := controllers.PerformBulkInsert(client, documentsBatch); err != nil {
			log.Printf("Bulk insert failed: %v", err)
		}
	}
}
