package parsers

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/elastic/go-grok"
	"github.com/google/uuid"
	"github.com/opensearch-project/opensearch-go"
	"github.com/rjohnsen/logwarp/src/controllers"
	"github.com/rjohnsen/logwarp/src/models"
)

func GrokParser(client *opensearch.Client, index string, filePath string, grokpattern string) {
	const bulkSize = 5000

	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var documentsBatch []models.Document

	groker := grok.New()
	err_grok := groker.Compile(grokpattern, true)

	if err_grok != nil {
		log.Fatalf("Unable to compile Grok statement: %s, %v", grokpattern, err)
	} else {
		for scanner.Scan() {
			line := scanner.Text()

			data, err := groker.ParseTypedString(line)

			if err != nil {
				log.Fatalf("Unable to parse line: %s", line)
			} else {

				documentId := uuid.New()

				document := models.Document{
					Metadata: models.IndexMetadataWrapper{
						Index: models.IndexMeta{
							IndexName:  index,
							DocumentID: documentId.String(),
						},
					},
					Source: models.DocumentSource{
						Content: data,
					},
				}

				if timestamp, ok := document.Source.Content["timestamp"].(string); ok && timestamp != "" {
					converted_time, _ := ConvertTimeFormat(timestamp)
					document.Source.Content["timestamp"] = converted_time
				} else {
					log.Printf("Warning: Missing or invalid timestamp in line: %s", line)
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
		}

		// Insert remaining documents if any.
		if len(documentsBatch) > 0 {
			if err := controllers.PerformBulkInsert(client, documentsBatch); err != nil {
				log.Printf("Bulk insert failed: %v", err)
			}
		}
	}
}

func ConvertTimeFormat(timestring string) (string, bool) {
	inputLayout := "02/Jan/2006:15:04:05 -0700"

	parsedTime, err := time.Parse(inputLayout, timestring)
	if err != nil {
		fmt.Printf("Failed to parse timestamp: %v\n", err)
		return "", true
	}

	return parsedTime.UTC().Format(time.RFC3339), false
}
