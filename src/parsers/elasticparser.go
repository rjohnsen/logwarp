// Package parsers handles parsing and bulk indexing of documents into OpenSearch.
package parsers

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"

	"github.com/rjohnsen/logwarp/src/controllers"
	"github.com/rjohnsen/logwarp/src/models"
	"github.com/rjohnsen/logwarp/src/syskernel"
)

// ElasticParser reads a file line by line, parses each line as JSON,
// and performs bulk inserts into OpenSearch.
func ElasticParser(appContext *syskernel.AppContext, jobid string, index string, filePath string) {
	jobstatus := syskernel.NatsJobStatus{
		Id:      jobid,
		Records: 0,
		Status:  0,
	}

	file, err := os.Open(filePath)
	if err != nil {
		error := syskernel.NatsJobError{Id: jobid, Error: fmt.Sprintf("Failed to open file: %v", err)}
		syskernel.SendError(appContext.Nats, error)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var documentsBatch []models.Document

	for scanner.Scan() {
		line := scanner.Text()
		var rawJSON map[string]interface{}

		// Parse JSON line into a temporary map to extract both index and source data.
		if err := json.Unmarshal([]byte(line), &rawJSON); err != nil {
			error := syskernel.NatsJobError{Id: jobid, Error: fmt.Sprintf("Failed to parse JSON: %v", err)}
			syskernel.SendError(appContext.Nats, error)
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
		if len(documentsBatch) >= appContext.Settings.OpenSearch.BulkSize {
			if err := controllers.PerformBulkInsert(appContext, &jobstatus, documentsBatch); err != nil {
				error := syskernel.NatsJobError{Id: jobid, Error: fmt.Sprintf("Bulk insert failed: %v", err)}
				syskernel.SendError(appContext.Nats, error)
			}
			documentsBatch = documentsBatch[:0] // Clear slice without reallocating.
		}
	}

	// Insert remaining documents if any.
	if len(documentsBatch) > 0 {
		if err := controllers.PerformBulkInsert(appContext, &jobstatus, documentsBatch); err != nil {
			error := syskernel.NatsJobError{Id: jobid, Error: fmt.Sprintf("Bulk insert failed: %v", err)}
			syskernel.SendError(appContext.Nats, error)
		}
	}
}
